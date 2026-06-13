use std::{error::Error, time::Duration};
use validator::{Validate, ValidationError};

#[derive(Debug, Clone, Validate)]
#[validate(schema(function = "validate_sizes", message = "Sizes must match"))]
pub struct Config {
    #[validate(length(min = 1, message = "POD_NAME cannot be empty"))]
    pub pod_name: String,

    #[validate(url(message = "DATABASE_URL must be a valid URL"))]
    pub db_url: String,

    #[validate(url(message = "PULSAR_URL must be a valid URL"))]
    pub pulsar_url: String,

    #[validate(range(min = 1, max = 10000))]
    pub batch_size: i64,

    pub poll_interval: Duration,
    pub pulsar_reconnect_delay: Duration,

    #[validate(range(min = 0, max = 100))]
    pub pulsar_max_retries: u32,

    #[validate(length(min = 1, message = "TOPIC_LIST cannot be empty"))]
    pub topics: Vec<String>,

    #[validate(length(min = 1, message = "TOPIC_TYPE_LIST cannot be empty"))]
    pub topics_type: Vec<String>,

    #[validate(range(min = 1, max = 500))]
    pub pg_max_connections: u32,

    #[validate(range(min = 1, max = 300))]
    pub pg_acquire_timeout_secs: u64,

    #[validate(range(min = 1, max = 5000))]
    pub pulsar_batch_size: u32,

    pub consumer_group: String,

    pub consumer_prefix: String,

    pub consumer_suffix: String,
}

fn validate_sizes(conf: &Config) -> Result<(), ValidationError> {
    if conf.topics.len() != conf.topics_type.len() {
        return Err(ValidationError::new("mismatched_lengths"));
    }
    Ok(())
}

impl Config {
    pub fn from_env() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let pod_name: String = std::env::var("POD_NAME")
            .map_err(|e: std::env::VarError| format!("POD_NAME is not set: {}", e))?;

        let db_url: String = std::env::var("DATABASE_URL")
            .map_err(|e: std::env::VarError| format!("DATABASE_URL is not set: {}", e))?;

        let pulsar_url: String = std::env::var("PULSAR_URL")
            .map_err(|e: std::env::VarError| format!("PULSAR_URL is not set: {}", e))?;

        let batch_size: i64 = std::env::var("BATCH_SIZE")
            .map_err(|e: std::env::VarError| format!("BATCH_SIZE is not set: {}", e))?
            .parse::<i64>()
            .map_err(|_| format!("BATCH_SIZE must be a valid integer"))?;

        let poll_interval: Duration = std::env::var("POLL_INTERVAL")
            .map_err(|e: std::env::VarError| format!("POLL_INTERVAL is not set: {}", e))?
            .parse::<u64>()
            .map(|secs: u64| Duration::from_secs(secs))
            .map_err(|_| format!("POLL_INTERVAL must be a positive integer"))?;

        let pulsar_reconnect_delay: Duration = std::env::var("PULSAR_RECONNECT_DELAY_SECS")
            .map_err(|e: std::env::VarError| {
                format!("PULSAR_RECONNECT_DELAY_SECS is not set: {}", e)
            })?
            .parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|_| format!("PULSAR_RECONNECT_DELAY_SECS must be a valid integer"))?;

        let pulsar_max_retries: u32 = std::env::var("PULSAR_MAX_RETRIES")
            .map_err(|e: std::env::VarError| format!("PULSAR_MAX_RETRIES is not set: {}", e))?
            .parse::<u32>()
            .map_err(|_| format!("PULSAR_MAX_RETRIES must be a valid integer"))?;

        let topic_list: String = std::env::var("TOPIC_LIST").map_err(|e: std::env::VarError| {
            format!("TOPIC_LIST is not set (expected comma-separated values like a,b,c)")
        })?;

        let topics: Vec<String> = topic_list
            .split(',')
            .map(|s: &str| s.trim().to_string())
            .filter(|s: &String| !s.is_empty())
            .collect();

        let topic_type_list: String =
            std::env::var("TOPIC_TYPE_LIST").map_err(|e: std::env::VarError| {
                format!("TOPIC_TYPE_LIST is not set (expected comma-separated values like a,b,c)")
            })?;

        let topics_type: Vec<String> = topic_type_list
            .split(',')
            .map(|s: &str| s.trim().to_string())
            .filter(|s: &String| !s.is_empty())
            .collect();

        let pg_max_connections: u32 = std::env::var("PG_MAX_CONNECTIONS")
            .map_err(|e: std::env::VarError| format!("PG_MAX_CONNECTIONS is not set: {}", e))?
            .parse::<u32>()
            .map_err(|_| format!("PG_MAX_CONNECTIONS must be a valid integer"))?;

        let pg_acquire_timeout_secs: u64 = std::env::var("PG_ACQUIRE_TIMEOUT_SECS")
            .map_err(|e: std::env::VarError| format!("PG_ACQUIRE_TIMEOUT_SECS is not set: {}", e))?
            .parse::<u64>()
            .map_err(|_| format!("PG_ACQUIRE_TIMEOUT_SECS must be a valid integer"))?;

        let pulsar_batch_size: u32 = std::env::var("PULSAR_BATCH_SIZE")
            .map_err(|e: std::env::VarError| format!("PULSAR_BATCH_SIZE is not set: {}", e))?
            .parse::<u32>()
            .map_err(|_| format!("PULSAR_BATCH_SIZE must be a valid integer"))?;

        let consumer_group: String = std::env::var("CONSUMER_GROUP")
            .map_err(|e: std::env::VarError| format!("CONSUMER_GROUP is not set: {}", e))?;

        let consumer_prefix: String = std::env::var("CONSUMER_PREFIX")
            .map_err(|e: std::env::VarError| format!("CONSUMER_PREFIX is not set: {}", e))?;

        let consumer_suffix: String = std::env::var("CONSUMER_SUFFIX")
            .map_err(|e: std::env::VarError| format!("CONSUMER_SUFFIX is not set: {}", e))?;

        let config: Config = Self {
            pod_name,
            db_url,
            pulsar_url,
            batch_size,
            poll_interval,
            pulsar_reconnect_delay,
            pulsar_max_retries,
            topics,
            topics_type,
            pg_max_connections,
            pg_acquire_timeout_secs,
            pulsar_batch_size,
            consumer_group,
            consumer_prefix,
            consumer_suffix,
        };

        config
            .validate()
            .map_err(|e: validator::ValidationErrors| {
                format!("Configuration validation failed: {}", e)
            })?;

        Ok(config)
    }
}
