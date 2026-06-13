use crate::{
    bootstrap::AppState,
    consumer::{MultiHandler, process_event_with_handler},
    model::EventEnveloped,
};

use futures::TryStreamExt;
use pulsar::{Consumer, Pulsar, SubType, TokioExecutor, consumer::DeadLetterPolicy};
use std::{error::Error, sync::Arc, time::Duration};
use tracing::{debug, error, info, warn};

pub async fn run_key_shared_consumer<L>(
    state: Arc<AppState>,
    topics: Vec<String>,
    handlers: Arc<L>,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    L: MultiHandler + Send + Sync + 'static,
{
    info!("KeyShared topics: {:?}", topics);

    let consumer_type: &str = "key-shared";

    // 1. Client Connection: Build and initialize the Pulsar gateway using the Tokio executor.
    let pulsar: Pulsar<_> = Pulsar::builder(&state.config.pulsar_url, TokioExecutor)
        .build()
        .await
        .map_err(|e: pulsar::Error| format!("Failed to create Pulsar client: {}", e))?;

    debug!("Connected to pulsar");

    // 2. Identity Setup: Define unique consumer names and shared group identifiers from config.
    let subscription_name: String = format!("{}-{}", state.config.consumer_group, consumer_type);
    let consumer_name: String = format!(
        "{}-{}-{}",
        state.config.consumer_prefix,
        state.config.pod_name,
        std::process::id()
    );

    // 3. Error Handling Policy: Configure Dead Letter Queue parameters for message redelivery limits.
    let consumer_dlq_policy: DeadLetterPolicy = DeadLetterPolicy {
        max_redeliver_count: 5,
        dead_letter_topic: format!("{}-{}-DLQ", state.config.consumer_group, consumer_type),
    };

    // 4. Consumer Initialization: Build the consumer with specific topics, subscription type, and DLQ policy.
    let mut consumer: Consumer<EventEnveloped, TokioExecutor> = pulsar
        .consumer()
        .with_topics(topics)
        .with_consumer_name(consumer_name)
        .with_subscription(&subscription_name)
        .with_subscription_type(SubType::KeyShared)
        .with_dead_letter_policy(consumer_dlq_policy)
        .build()
        .await
        .map_err(|e: pulsar::Error| format!("Failed to create Pulsar consumer: {}", e))?;

    info!("KeyShared loop started");

    // 5. Message Processing Loop: Continuously poll the consumer for new messages from Pulsar.
    while let Some(msg) = consumer.try_next().await? {
        // 6. Data Deserialization: Parse the payload and acknowledge malformed messages to avoid blocking.
        let event: EventEnveloped = match msg.deserialize() {
            Ok(data) => data,
            Err(e) => {
                error!("Could not deserialize message: {:?}", e);
                consumer.ack(&msg).await?;
                continue;
            }
        };

        // 7. Route Filtering: Validate if the current handler implementation supports the event's entity type.
        if !handlers.can_handle(&event.entity_type) {
            info!("Event {} ignored by {}", event.event_id, handlers.name());
            consumer.ack(&msg).await?;
            continue;
        }

        // 8. Execution and Ack: Process the event and acknowledge on success or nack with delay on failure.
        match process_event_with_handler(&state, &event, handlers.as_ref()).await {
            Ok(processed) => {
                if processed {
                    info!(id = %event.event_id, "Event processed successfully");
                } else {
                    warn!(id = %event.event_id, "Event skipped (already processed)");
                }
                consumer.ack(&msg).await?;
            }
            Err(e) => {
                error!(id = %event.event_id, "Critical error processing event: {:?}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                consumer.nack(&msg).await?;
            }
        }
    }

    Ok(())
}
