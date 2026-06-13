use anyhow::Result;
use event_publisher::{
    application,
    infrastructure::bootstrap::{self, AppState},
};
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let state: std::sync::Arc<AppState> = bootstrap::run().await?;

    tokio::select! {
    res = application::run(state.clone()) => {
        match res {
            Ok(_) => {
                warn!("Application loop finished gracefully but unexpectedly");
            },
            Err(e) => {
                error!(
                error = %e,
                cause = ?e.source(),
                "Application loop CRASHED"
                );

                return Err(e);
            }
        }
    },

    _ = tokio::signal::ctrl_c() => {
         info!("Ctrl+C signal received, initiating graceful shutdown");
    },
    }

    info!("Service stopped");

    Ok(())
}
