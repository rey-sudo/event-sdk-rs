use event_consumer::{
    application::{self, consumer::MultiHandler},
    async_trait,
    infrastructure::bootstrap::{self, AppState},
    model::EventEnveloped,
    sqlx::{Postgres, Transaction},
};
use std::error::Error;

use tracing::{error, info, warn};

/// EXAMPLE. A specific handler implementation for folder-related events.
/// This struct encapsulates the domain logic for the "folder" entity type.
struct FolderHandler;

#[async_trait]
impl MultiHandler for FolderHandler {
    /// Determines if this handler should process a given entity_type.
    /// Used by the dispatcher to route events only to interested parties.    
    fn can_handle(&self, entity_type: &str) -> bool {
        entity_type == "folder"
    }
    /// Returns a human-readable name for the handler.
    /// Primarily used for structured logging and debugging purposes.
    fn name(&self) -> &str {
        "FolderHandler"
    }
    /// Executes the core business logic for a specific event.
    /// * `tx` - A mutable reference to an active SQLx transaction.
    /// * `event` - The enveloped event containing metadata and the actual payload.
    async fn handle<'a>(
        &self,
        tx: &mut Transaction<'a, Postgres>,
        event: &EventEnveloped,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match event.event_type.as_str() {
            "folder.created" => {
                info!("Handling folder creation for ID: {}", event.event_id);
                // self.on_created(tx, event).await
                Ok(())
            }
            "folder.updated" => {
                info!("Handling folder update for ID: {}", event.event_id);
                // self.on_updated(tx, event).await
                Ok(())
            }
            "folder.deleted" => {
                info!("Handling folder deletion for ID: {}", event.event_id);
                // self.on_deleted(tx, event).await
                Ok(())
            }
            _ => {
                // If we don't care about this specific action, we ACK and move on
                warn!("No specific logic for event_type: {}", event.event_type);
                Ok(())
            }
        }
    }
}

/// EXAMPLE. A specific handler implementation for folder-related events.
/// This struct encapsulates the domain logic for the "folder" entity type.
struct DocumentHandler;

#[async_trait]
impl MultiHandler for DocumentHandler {
    /// Determines if this handler should process a given entity_type.
    /// Used by the dispatcher to route events only to interested parties.    
    fn can_handle(&self, entity_type: &str) -> bool {
        entity_type == "document"
    }
    /// Returns a human-readable name for the handler.
    /// Primarily used for structured logging and debugging purposes.
    fn name(&self) -> &str {
        "DocumentHandler"
    }
    /// Executes the core business logic for a specific event.
    /// * `tx` - A mutable reference to an active SQLx transaction.
    /// * `event` - The enveloped event containing metadata and the actual payload.
    async fn handle<'a>(
        &self,
        tx: &mut Transaction<'a, Postgres>,
        event: &EventEnveloped,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match event.event_type.as_str() {
            "document.created" => {
                info!("Handling document.created for ID: {}", event.event_id);
                // self.on_created(tx, event).await
                Ok(())
            }

            "document.processed" => {
                info!("Handling document.processed for ID: {}", event.event_id);
                // self.on_created(tx, event).await
                Ok(())
            }
            _ => {
                // If we don't care about this specific action, we ACK and move on
                warn!("No specific logic for event_type: {}", event.event_type);
                Ok(())
            }
        }
    }
}

//  ROUTER ---------------------------------------------------------------------------------------
/// This struct follows the Router/Dispatcher pattern, allowing a single
/// consumer to manage multiple entity types efficiently.
struct Router {
    folder: FolderHandler,
    document: DocumentHandler,
}

#[async_trait]
impl MultiHandler for Router {
    /// Returns true if at least one sub-handler is interested in the entity type.
    fn can_handle(&self, entity_type: &str) -> bool {
        self.folder.can_handle(entity_type) || self.document.can_handle(entity_type)
    }
    /// Returns the identifier for this router.
    /// Useful for identifying the dispatcher in high-level application logs.
    fn name(&self) -> &str {
        "Router"
    }

    /// Matches the event's entity type against the available handlers and
    /// ensures the database transaction (`tx`) is passed down correctly.
    async fn handle<'a>(
        &self,
        tx: &mut Transaction<'a, Postgres>,
        event: &EventEnveloped,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match event.entity_type.as_str() {
            "folder" => self.folder.handle(tx, event).await,
            "document" => self.document.handle(tx, event).await,
            _ => {
                warn!(
                    "MultiHandler received type it cannot route: {}",
                    event.entity_type
                );
                Ok(())
            }
        }
    }
}

// MAIN ---------------------------------------------------------------------------------------
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // 1. Bootstrap: Init config, DB pools, and shared resources in Arc for thread-safety.
    let state: std::sync::Arc<AppState> = bootstrap::run().await?;

    // 2. Business Logic Handlers: Instance of the specific handlers for this service.
    let multi_handler: Router = Router {
        folder: FolderHandler,
        document: DocumentHandler,
    };

    // 3. Concurrent Flow Control: 'tokio::select!' monitors multiple futures simultaneously.
    tokio::select! {
        res = application::run(state, multi_handler) => {
            match res {
                Ok(_) => warn!("Application loop finished gracefully but unexpectedly"),
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
