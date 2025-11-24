use etl::{
    config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig},
    pipeline::Pipeline,
    store::both::memory::MemoryStore,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::Executor;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

// Re-export from crate for testing
use event_router::destination::EventRouterDestination;
use event_router::models::Operation;
use event_router::subscriptions::{match_db_subs, match_event_subs, SUBSCRIPTIONS};

const MIGRATIONS_PATH: &str = "../../migrations";

/// Apply pgconductor migrations and setup CDC
async fn setup_database(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    // Create schema first
    pool.execute("CREATE SCHEMA IF NOT EXISTS pgconductor;").await?;

    // Read and apply base migrations (includes subscriptions and events tables)
    let migrations_sql = std::fs::read_to_string(format!("{}/0000000001_setup.sql", MIGRATIONS_PATH))
        .expect("Failed to read migrations");

    pool.execute(migrations_sql.as_str()).await?;

    // Setup CDC for subscriptions and events tables
    pool.execute(
        r#"
        -- Create publication for CDC
        CREATE PUBLICATION pgconductor_events FOR TABLE pgconductor.events, pgconductor.subscriptions;
        "#,
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_load_subscriptions_on_startup() {
    // Start postgres container with version 15+
    let container = Postgres::default()
        .with_tag("15-alpine")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let database_url = format!(
        "postgres://postgres:postgres@127.0.0.1:{}/postgres",
        port
    );

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .unwrap();

    // Setup database
    setup_database(&pool).await.unwrap();

    // Insert test subscriptions
    let exec_1 = Uuid::new_v4();
    let exec_2 = Uuid::new_v4();
    let exec_3 = Uuid::new_v4();

    sqlx::query(
        r#"
        INSERT INTO pgconductor.subscriptions (source, schema_name, table_name, operation, execution_id)
        VALUES ('db', 'public', 'users', 'insert', $1);
        "#,
    )
    .bind(exec_1)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
        INSERT INTO pgconductor.subscriptions (source, event_key, execution_id)
        VALUES ('event', 'user.verified', $1);
        "#,
    )
    .bind(exec_2)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
        INSERT INTO pgconductor.subscriptions (source, schema_name, table_name, operation, execution_id)
        VALUES ('db', 'public', 'users', 'insert', $1);
        "#,
    )
    .bind(exec_3)
    .execute(&pool)
    .await
    .unwrap();

    // Create destination - should load subscriptions
    let _destination = EventRouterDestination::new(pool.clone()).await.unwrap();

    // Verify subscriptions were loaded
    let db_subs = match_db_subs("public", "users", Operation::Insert);
    assert!(db_subs.is_some());
    assert_eq!(db_subs.unwrap().len(), 2); // exec-1 and exec-3

    let event_subs = match_event_subs("user.verified").expect("event sub should exist");
    assert_eq!(event_subs.len(), 1);
    assert_eq!(event_subs[0].execution_id, Some(exec_2));
}

#[tokio::test]
async fn test_no_subscriptions_returns_none() {
    let container = Postgres::default()
        .with_tag("15-alpine")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let database_url = format!(
        "postgres://postgres:postgres@127.0.0.1:{}/postgres",
        port
    );

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .unwrap();

    setup_database(&pool).await.unwrap();

    // Create destination with no subscriptions
    let _destination = EventRouterDestination::new(pool.clone()).await.unwrap();

    // Verify no matches
    assert!(match_db_subs("public", "users", Operation::Insert).is_none());
    assert!(match_event_subs("nonexistent").is_none());
}

#[tokio::test]
async fn test_multiple_operations_same_table() {
    let container = Postgres::default()
        .with_tag("15-alpine")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let database_url = format!(
        "postgres://postgres:postgres@127.0.0.1:{}/postgres",
        port
    );

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .unwrap();

    setup_database(&pool).await.unwrap();

    // Insert subscriptions for different operations
    let exec_insert = Uuid::new_v4();
    let exec_update = Uuid::new_v4();
    let exec_delete = Uuid::new_v4();

    sqlx::query(
        r#"
        INSERT INTO pgconductor.subscriptions (source, schema_name, table_name, operation, execution_id)
        VALUES
            ('db', 'public', 'orders', 'insert', $1),
            ('db', 'public', 'orders', 'update', $2),
            ('db', 'public', 'orders', 'delete', $3);
        "#,
    )
    .bind(exec_insert)
    .bind(exec_update)
    .bind(exec_delete)
    .execute(&pool)
    .await
    .unwrap();

    let _destination = EventRouterDestination::new(pool.clone()).await.unwrap();

    // Verify each operation has its own subscription
    let insert_subs = match_db_subs("public", "orders", Operation::Insert).unwrap();
    assert_eq!(insert_subs.len(), 1);
    assert_eq!(insert_subs[0].execution_id, Some(exec_insert));

    let update_subs = match_db_subs("public", "orders", Operation::Update).unwrap();
    assert_eq!(update_subs.len(), 1);
    assert_eq!(update_subs[0].execution_id, Some(exec_update));

    let delete_subs = match_db_subs("public", "orders", Operation::Delete).unwrap();
    assert_eq!(delete_subs.len(), 1);
    assert_eq!(delete_subs[0].execution_id, Some(exec_delete));
}

#[tokio::test]
async fn test_subscription_index_structure() {
    let container = Postgres::default()
        .with_tag("15-alpine")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let database_url = format!(
        "postgres://postgres:postgres@127.0.0.1:{}/postgres",
        port
    );

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .unwrap();

    setup_database(&pool).await.unwrap();

    let exec_1 = Uuid::new_v4();
    let exec_2 = Uuid::new_v4();

    sqlx::query(
        r#"
        INSERT INTO pgconductor.subscriptions (source, schema_name, table_name, operation, execution_id)
        VALUES ('db', 'myschema', 'mytable', 'insert', $1);
        "#,
    )
    .bind(exec_1)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
        INSERT INTO pgconductor.subscriptions (source, event_key, execution_id)
        VALUES ('event', 'order.created', $1);
        "#,
    )
    .bind(exec_2)
    .execute(&pool)
    .await
    .unwrap();

    let _destination = EventRouterDestination::new(pool.clone()).await.unwrap();

    // Verify index structure
    let idx = SUBSCRIPTIONS.load();
    let idx = idx.as_ref().expect("Index should be loaded");
    assert_eq!(idx.by_id.len(), 2);
    assert_eq!(idx.db_subs.len(), 1);
    assert_eq!(idx.event_subs.len(), 1);
}

#[tokio::test]
async fn test_full_pipeline_with_cdc() {
    // Start postgres with logical replication enabled
    let container = Postgres::default()
        .with_tag("15-alpine")
        .with_cmd([
            "postgres",
            "-c", "wal_level=logical",
            "-c", "max_replication_slots=10",
            "-c", "max_wal_senders=10",
        ])
        .start()
        .await
        .unwrap();

    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let host = "127.0.0.1".to_string();
    let database = "postgres".to_string();
    let username = "postgres".to_string();
    let password = "postgres".to_string();

    let database_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        username, password, host, port, database
    );

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .unwrap();

    // Setup database
    setup_database(&pool).await.unwrap();

    // Insert initial subscription before starting pipeline
    let exec_initial = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO pgconductor.subscriptions (source, schema_name, table_name, operation, execution_id)
        VALUES ('db', 'public', 'orders', 'insert', $1);
        "#,
    )
    .bind(exec_initial)
    .execute(&pool)
    .await
    .unwrap();

    // Create destination (loads initial subscriptions)
    let destination = EventRouterDestination::new(pool.clone()).await.unwrap();

    // Verify initial subscription loaded
    assert!(match_db_subs("public", "orders", Operation::Insert).is_some());

    // Create ETL pipeline config
    let pg_config = PgConnectionConfig {
        host: host.clone(),
        port,
        name: database.clone(),
        username: username.clone(),
        password: Some(password.clone().into()),
        tls: TlsConfig {
            enabled: false,
            trusted_root_certs: String::new(),
        },
    };

    let pipeline_config = PipelineConfig {
        id: 1,
        publication_name: "pgconductor_events".into(),
        pg_connection: pg_config,
        batch: BatchConfig {
            max_size: 100,
            max_fill_ms: 50, // Fast batching for tests
        },
        table_error_retry_delay_ms: 1000,
        table_error_retry_max_attempts: 3,
        max_table_sync_workers: 2,
    };

    let store = MemoryStore::new();
    let mut pipeline = Pipeline::new(pipeline_config, store, destination);

    // Start pipeline
    pipeline.start().await.unwrap();

    // Give pipeline time to initialize
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Insert new subscription via CDC
    let exec_cdc_1 = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO pgconductor.subscriptions (source, event_key, execution_id)
        VALUES ('event', 'payment.completed', $1);
        "#,
    )
    .bind(exec_cdc_1)
    .execute(&pool)
    .await
    .unwrap();

    // Wait for CDC to process
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Verify CDC subscription was added to index
    let event_subs = match_event_subs("payment.completed");
    assert!(event_subs.is_some(), "CDC subscription should be in index");
    assert_eq!(event_subs.unwrap()[0].execution_id, Some(exec_cdc_1));

    // Insert another DB subscription
    let exec_cdc_2 = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO pgconductor.subscriptions (source, schema_name, table_name, operation, execution_id)
        VALUES ('db', 'public', 'users', 'update', $1);
        "#,
    )
    .bind(exec_cdc_2)
    .execute(&pool)
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Verify it was added
    let db_subs = match_db_subs("public", "users", Operation::Update);
    assert!(db_subs.is_some());
    assert_eq!(db_subs.unwrap()[0].execution_id, Some(exec_cdc_2));

    // Delete a subscription
    sqlx::query(
        r#"
        DELETE FROM pgconductor.subscriptions WHERE execution_id = $1;
        "#,
    )
    .bind(exec_initial)
    .execute(&pool)
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Verify it was removed
    let removed = match_db_subs("public", "orders", Operation::Insert);
    assert!(removed.is_none(), "Deleted subscription should be removed from index");

    // Stop pipeline gracefully
    drop(pipeline);
}

#[tokio::test]
async fn test_cdc_subscription_update() {
    let container = Postgres::default()
        .with_tag("15-alpine")
        .with_cmd([
            "postgres",
            "-c", "wal_level=logical",
            "-c", "max_replication_slots=10",
            "-c", "max_wal_senders=10",
        ])
        .start()
        .await
        .unwrap();

    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let database_url = format!(
        "postgres://postgres:postgres@127.0.0.1:{}/postgres",
        port
    );

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .unwrap();

    setup_database(&pool).await.unwrap();

    // Insert subscription
    let sub_id = Uuid::new_v4();
    let exec_original = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO pgconductor.subscriptions (id, source, event_key, execution_id)
        VALUES ($1, 'event', 'order.placed', $2);
        "#,
    )
    .bind(sub_id)
    .bind(exec_original)
    .execute(&pool)
    .await
    .unwrap();

    let destination = EventRouterDestination::new(pool.clone()).await.unwrap();

    // Verify initial
    let subs = match_event_subs("order.placed").unwrap();
    assert_eq!(subs[0].execution_id, Some(exec_original));

    // Setup pipeline
    let pg_config = PgConnectionConfig {
        host: "127.0.0.1".into(),
        port,
        name: "postgres".into(),
        username: "postgres".into(),
        password: Some("postgres".to_string().into()),
        tls: TlsConfig {
            enabled: false,
            trusted_root_certs: String::new(),
        },
    };

    let pipeline_config = PipelineConfig {
        id: 2,
        publication_name: "pgconductor_events".into(),
        pg_connection: pg_config,
        batch: BatchConfig {
            max_size: 100,
            max_fill_ms: 50,
        },
        table_error_retry_delay_ms: 1000,
        table_error_retry_max_attempts: 3,
        max_table_sync_workers: 2,
    };

    let store = MemoryStore::new();
    let mut pipeline = Pipeline::new(pipeline_config, store, destination);
    pipeline.start().await.unwrap();

    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Update subscription execution_id
    let exec_updated = Uuid::new_v4();
    sqlx::query(
        r#"
        UPDATE pgconductor.subscriptions
        SET execution_id = $1
        WHERE id = $2;
        "#,
    )
    .bind(exec_updated)
    .bind(sub_id)
    .execute(&pool)
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Verify update was processed
    let updated_subs = match_event_subs("order.placed").unwrap();
    assert_eq!(updated_subs[0].execution_id, Some(exec_updated));

    drop(pipeline);
}
