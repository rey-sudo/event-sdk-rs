use crate::config::Config;
use dotenvy::from_filename;
use sqlx::postgres::PgPoolOptions;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub pool: sqlx::PgPool,
}

pub async fn run() -> Result<Arc<AppState>, Box<dyn Error + Send + Sync>> {
    from_filename(".env").ok();

    // 1. Init tracing subscriber for structured, async-safe logging and service-wide telemetry.
    tracing_subscriber::fmt::init();

    // 2. Returns an error if any required variables are missing or malformed.
    let config: Config = Config::from_env()?;

    // 3. Initialize a connection pool to PostgreSQL with maximum concurrency and acquisition timeout limits.
    let pool: sqlx::Pool<sqlx::Postgres> = PgPoolOptions::new()
        .max_connections(config.pg_max_connections)
        .acquire_timeout(Duration::from_secs(config.pg_acquire_timeout_secs))
        .connect(&config.db_url)
        .await
        .map_err(|e: sqlx::Error| format!("Error connecting to PostgreSQL: {}", e))?;

    info!("Bootstrap finished");

    Ok(Arc::new(AppState { config, pool }))
}
