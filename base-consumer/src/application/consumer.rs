use crate::{infrastructure::bootstrap::AppState, model::EventEnveloped};
use async_trait::async_trait;
use sqlx::{Postgres, Transaction};
use std::{error::Error, sync::Arc};
use tracing::error;

/// Contract for event processing components.
/// Requires thread-safety and supports asynchronous execution.
#[async_trait]
pub trait MultiHandler: Send + Sync {
    /// 1. Capability Check: Determines if this handler can process a specific entity type.
    fn can_handle(&self, entity_type: &str) -> bool;

    /// 2. Logic Execution: Processes the business logic within a shared database transaction.
    /// Returns an error to trigger a rollback and signal a retry (NACK).
    async fn handle<'a>(
        &self,
        tx: &mut Transaction<'a, Postgres>,
        event: &EventEnveloped,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// 3. Identification: Provides a unique name for telemetry and debugging purposes.
    fn name(&self) -> &str;
}

/// Core engine for transactional event processing.
/// Implements the Inbox Pattern to ensure "Exactly-Once" semantics within the database.
pub async fn process_event_with_handler<L: MultiHandler>(
    state: &Arc<AppState>,
    event: &EventEnveloped,
    handlers: &L,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    // 1. Transaction Initialization: Begins an atomic unit of work to ensure data integrity.
    let mut tx: Transaction<'_, Postgres> = state
        .pool
        .begin()
        .await
        .map_err(|e: sqlx::Error| format!("Failed to begin transaction: {}", e))?;

    let now_ms: i64 = chrono::Utc::now().timestamp_millis();

    // 2. Idempotency Control: Attempts to record the event to prevent duplicate processing.
    let result: sqlx::postgres::PgQueryResult = sqlx::query(
        "INSERT INTO processed_events (event_id, created_at) 
            VALUES ($1, $2) 
            ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(&event.event_id)
    .bind(now_ms)
    .execute(&mut *tx)
    .await?;

    // 3. Duplicate Detection: Skips execution if the event was already handled (0 rows affected).
    if result.rows_affected() == 0 {
        return Ok(false); // Already processed
    }

    // 4. Handler Dispatch: Delegates the event processing to the specific handler implementation.
    if let Err(e) = handlers.handle(&mut tx, event).await {
        error!(
            "Business logic failed for event {}: {:?}",
            event.event_id, e
        );
        // 5. Automatic Rollback: Returning an error causes the transaction to drop and roll back.
        return Err(e);
    }

    // 6. Final Commit: Persists both the idempotency record and business changes simultaneously.
    tx.commit()
        .await
        .map_err(|e: sqlx::Error| format!("Failed to commit transaction: {}", e))?;

    Ok(true)
}
