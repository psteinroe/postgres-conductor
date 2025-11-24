use event_router::destination::EventRouterDestination;
use etl::{
    config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig},
    pipeline::Pipeline,
    store::both::memory::MemoryStore,
};
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("event_router=debug".parse()?))
        .init();

    // Build database URL
    let host = std::env::var("PGHOST").unwrap_or_else(|_| "localhost".into());
    let port = std::env::var("PGPORT").unwrap_or_else(|_| "5432".into());
    let database = std::env::var("PGDATABASE").unwrap_or_else(|_| "postgres".into());
    let username = std::env::var("PGUSER").unwrap_or_else(|_| "postgres".into());
    let password = std::env::var("PGPASSWORD").unwrap_or_else(|_| "".into());

    let database_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        username, password, host, port, database
    );

    // Create sqlx pool for querying subscriptions
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    // Database connection config for etl
    let pg = PgConnectionConfig {
        host,
        port: port.parse().unwrap_or(5432),
        name: database,
        username,
        password: if password.is_empty() { None } else { Some(password.into()) },
        tls: TlsConfig {
            enabled: false,
            trusted_root_certs: String::new(),
        },
    };

    let store = MemoryStore::new();

    // Create destination and load subscriptions from DB
    let destination = EventRouterDestination::new(pool).await?;
    tracing::info!("Subscriptions loaded, starting pipeline");

    let config = PipelineConfig {
        id: 1,
        publication_name: std::env::var("PUBLICATION_NAME")
            .unwrap_or_else(|_| "pgconductor_events".into()),
        pg_connection: pg,
        batch: BatchConfig {
            max_size: 1000,
            max_fill_ms: 100, // Low latency for events
        },
        table_error_retry_delay_ms: 10_000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
    };

    let mut pipeline = Pipeline::new(config, store, destination);
    pipeline.start().await?;

    pipeline.wait().await?;

    Ok(())
}
