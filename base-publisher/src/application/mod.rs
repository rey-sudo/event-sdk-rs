pub mod publisher;

use crate::{application::publisher::publish_pending_events, infrastructure::bootstrap::AppState};
use anyhow::{Context, Result};
use pulsar::{Producer, Pulsar, TokioExecutor};
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::time::sleep;
use tracing::{error, info};

pub type ProducerCache = HashMap<String, Producer<TokioExecutor>>;

pub async fn run(state: Arc<AppState>) -> Result<()> {
    info!("Starting worker...");

    // Initialize the Pulsar client using the configured URL and the Tokio async executor.
    let pulsar: Pulsar<_> = Pulsar::builder(&state.config.pulsar_url, TokioExecutor)
        .build()
        .await
        .context("Failed to create Pulsar client")?;

    // Initialize an in-memory cache to reuse Pulsar producers and avoid redundant handshakes.
    let mut producers: ProducerCache = HashMap::new();
   
    loop {
        match publish_pending_events(&state, &pulsar, &mut producers).await {
            Ok(count) if count > 0 => {
                info!("Published {} events", count);
            }
            Ok(_) => {
                sleep(state.config.poll_interval).await;
            }
            Err(e) => {
                error!("Error in application loop: {:?}", e);
                sleep(Duration::from_secs(5)).await; 
            }
        }
    }
}
