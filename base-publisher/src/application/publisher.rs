use crate::{application::ProducerCache, infrastructure::bootstrap::AppState};
use anyhow::Result;
use pulsar::{Pulsar, TokioExecutor};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

#[derive(sqlx::FromRow, Debug)]
struct EventRow {
    pub event_type: String,
    pub id: Uuid,
    pub entity_type: String,
    pub entity_id: String,
    pub data: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EventEnvelope {
    pub event_id: Uuid,
    pub event_type: String,
    pub entity_type: String,
    pub data: serde_json::Value,
}

pub async fn publish_pending_events(
    state: &Arc<AppState>,
    pulsar: &Pulsar<TokioExecutor>,
    producers: &mut ProducerCache,
) -> Result<usize> {
    // 1. Fetch unpublished events with a row-level lock, skipping already locked rows.
    let rows: Vec<EventRow> = sqlx::query_as::<_, EventRow>(
        "SELECT event_type, id, entity_type, entity_id, data FROM events 
         WHERE published = FALSE 
         ORDER BY time ASC 
         LIMIT $1 FOR UPDATE SKIP LOCKED",
    )
    .bind(state.config.batch_size)
    .fetch_all(&state.pool)
    .await?;

    if rows.is_empty() {
        return Ok(0);
    }

    let mut published_count: usize = 0;

    for row in rows {
        //2. Search the topic by event_type
        let pattern = format!("/{}", row.event_type);

        let target_topic = state
            .config
            .topics
            .iter()
            .find(|&t| t.ends_with(&pattern) || t == &row.event_type) 
            .unwrap_or(&state.config.topics[0]);

        //3. Cache a Pulsar producer for the target topic if it does not already exist in the registry.
        if !producers.contains_key(target_topic) {
            let new_producer: pulsar::Producer<TokioExecutor> =
                pulsar.producer().with_topic(target_topic).build().await?;

            producers.insert(target_topic.clone(), new_producer);
            info!("Producer cached")
        }
        //4. Retrieve the mutable producer from the cache.
        let producer: &mut pulsar::Producer<TokioExecutor> =
            producers.get_mut(target_topic).unwrap();

        let envelope: EventEnvelope = EventEnvelope {
            event_id: row.id.clone(),
            event_type: row.event_type.clone(),
            entity_type: row.entity_type.clone(),
            data: row.data.clone(),
        };

        let payload: Vec<u8> = serde_json::to_vec(&envelope)?;

        let message: pulsar::producer::Message = pulsar::producer::Message {
            payload,
            partition_key: Some(row.entity_id.clone()),
            ..Default::default()
        };

        //5. Dispatch the event to Pulsar and upon successful delivery mark the event as published.
        match producer.send_non_blocking(message).await {
            Ok(_) => {
                sqlx::query("UPDATE events SET published = TRUE WHERE id = $1")
                    .bind(&row.id)
                    .execute(&state.pool)
                    .await?;

                published_count += 1;
                info!("Event send {} - {}", target_topic, envelope.entity_type);
            }
            Err(e) => error!("Failed to send event {} to pulsar: {:?}", envelope.event_id, e),
        }
    }

    Ok(published_count)
}
