Event Publisher for Pulsar & PostgreSQL
A high-performance Rust worker that polls events from a PostgreSQL table and publishes them to Apache Pulsar with at-least-once delivery semantics.

Features
Transactional Outbox Pattern: Reliably move events from Postgres to Pulsar.

High Concurrency: Uses sqlx with SKIP LOCKED to allow multiple instances to run in parallel without double-publishing.

Producer Caching: Reuses Pulsar producers to minimize handshake overhead.

Type-Based Routing: Automatically routes events to topics based on their entity_type.

Fail-Safe: Environment-based configuration with built-in validation.

Quick Start
1. Database Schema
Your PostgreSQL table should follow this structure:

SQL

CREATE TABLE events (
    id UUID PRIMARY KEY,
    entity_type VARCHAR(255) NOT NULL,
    data JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    time BIGINT NOT NULL
);

CREATE INDEX idx_events_unpublished ON events (time) WHERE published = FALSE;
2. Configuration
Set the following environment variables:

Bash

DATABASE_URL=postgres://user:pass@localhost/db
PULSAR_URL=pulsar://127.0.0.1:6650
TOPIC_LIST=persistent://public/default/users.events,persistent://public/default/orders.events
POLL_INTERVAL=5
3. Usage
Rust

use event_publisher::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    // Your worker initialization logic here...
    Ok(())
}
License
GNU GPL v3.0 