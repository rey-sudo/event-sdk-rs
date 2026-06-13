use crate::config::Config;
use anyhow::Context;
use anyhow::Result;
use dotenvy::from_filename;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub pool: sqlx::PgPool,
}

pub async fn run() -> Result<Arc<AppState>> {
    from_filename(".env").ok();

    // Initialize tracing subscriber for structured, async-safe logging.
    // Enables info!, warn!, error! logs across the entire service.
    tracing_subscriber::fmt::init();

    // Returns an error if any required variables are missing or malformed.
    let config: Config = Config::from_env()?;

    // Initialize a connection pool to PostgreSQL with maximum concurrency and acquisition timeout limits.
    let pool: sqlx::Pool<sqlx::Postgres> = PgPoolOptions::new()
        .max_connections(config.pg_max_connections)
        .acquire_timeout(Duration::from_secs(config.pg_acquire_timeout_secs))
        .connect(&config.db_url)
        .await
        .context("Error connecting to PostgreSQL")?;

    info!("Bootstrap finished");

    Ok(Arc::new(AppState { config, pool }))
}
