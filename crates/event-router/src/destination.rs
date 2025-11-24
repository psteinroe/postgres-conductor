use crate::models::{DbSubscription, EventSubscription, Operation, Subscription};
use crate::subscriptions::{match_db_subs, match_event_subs, rebuild_index, SUBSCRIPTIONS};
use etl::destination::Destination;
use etl::error::EtlResult;
use etl::types::{ArrayCell, Cell, Event, TableId, TableRow};
use parking_lot::Mutex;
use sqlx::postgres::PgPool;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Table names we care about
const SUBSCRIPTIONS_TABLE: &str = "subscriptions";
const EVENTS_TABLE: &str = "events";
const PGCONDUCTOR_SCHEMA: &str = "pgconductor";

/// Event router destination that:
/// 1. Maintains in-memory index of subscriptions via CDC
/// 2. Routes DB events and custom events to waiting executions
#[derive(Clone)]
pub struct EventRouterDestination {
    /// Database pool for querying subscriptions on startup
    pool: PgPool,
    /// Whether subscriptions have been loaded
    initialized: Arc<AtomicBool>,
    /// Table ID to (schema, name, column_names) mapping
    table_names: Arc<Mutex<HashMap<TableId, (String, String, Vec<String>)>>>,
}

impl EventRouterDestination {
    /// Create a new EventRouterDestination and load subscriptions from DB
    pub async fn new(pool: PgPool) -> Result<Self, sqlx::Error> {
        let dest = Self {
            pool,
            initialized: Arc::new(AtomicBool::new(false)),
            table_names: Arc::new(Mutex::new(HashMap::new())),
        };

        // Load subscriptions from database on startup
        dest.load_subscriptions().await?;
        dest.initialized.store(true, Ordering::Release);

        Ok(dest)
    }

    /// Load all subscriptions from the database
    async fn load_subscriptions(&self) -> Result<(), sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT id, source, schema_name, table_name, operation, event_key, execution_id, queue, step_key, task_key, columns
            FROM pgconductor.subscriptions
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut subs = Vec::with_capacity(rows.len());

        for row in rows {
            let id: Uuid = row.get("id");
            let source: &str = row.get("source");
            let execution_id: Option<Uuid> = row.get("execution_id");
            let queue: String = row.get("queue");
            let step_key: Option<String> = row.get("step_key");
            let task_key: Option<String> = row.get("task_key");

            let sub = match source {
                "db" => {
                    let schema_name: String = row.get("schema_name");
                    let table_name: String = row.get("table_name");
                    let operation: &str = row.get("operation");
                    let columns: Option<Vec<String>> = row.get("columns");
                    let op = match operation {
                        "insert" => Operation::Insert,
                        "update" => Operation::Update,
                        "delete" => Operation::Delete,
                        _ => continue,
                    };

                    Subscription::Db(DbSubscription {
                        id,
                        schema_name: Arc::from(schema_name),
                        table_name: Arc::from(table_name),
                        op,
                        execution_id,
                        queue: Arc::from(queue),
                        step_key: step_key.as_ref().map(|s| Arc::from(s.as_str())),
                        task_key: task_key.as_ref().map(|s| Arc::from(s.as_str())),
                        columns: columns.map(|cols| cols.into_iter().map(|c| Arc::from(c.as_str())).collect()),
                    })
                }
                "event" => {
                    let event_key: String = row.get("event_key");
                    Subscription::Event(EventSubscription {
                        id,
                        event_key: Arc::from(event_key),
                        execution_id,
                        queue: Arc::from(queue),
                        step_key: step_key.map(|s| Arc::from(s.as_str())),
                        task_key: task_key.map(|s| Arc::from(s.as_str())),
                    })
                }
                _ => continue,
            };

            subs.push(sub);
        }

        info!(count = subs.len(), "Loaded subscriptions from database");
        rebuild_index(subs);

        Ok(())
    }

    /// Check if initialized
    pub fn is_ready(&self) -> bool {
        self.initialized.load(Ordering::Acquire)
    }

    /// Parse a subscription row from the database
    fn parse_subscription_row(&self, row: &TableRow) -> Option<Subscription> {
        // Expected columns: id, source, schema_name, table_name, operation, event_key, execution_id, queue, step_key, task_key, columns
        if row.values.len() < 8 {
            warn!("Invalid subscription row: not enough columns");
            return None;
        }

        let id = match &row.values[0] {
            Cell::Uuid(u) => *u,
            Cell::String(s) => s.parse().ok()?,
            _ => return None,
        };

        let source = match &row.values[1] {
            Cell::String(s) => s.as_str(),
            _ => return None,
        };

        // execution_id is at index 6, may be null for trigger-based subscriptions
        let execution_id = match &row.values[6] {
            Cell::Uuid(u) => Some(*u),
            Cell::String(s) => Some(s.parse().ok()?),
            Cell::Null => None,
            _ => return None,
        };

        let queue = match &row.values[7] {
            Cell::String(s) => Arc::from(s.as_str()),
            _ => return None,
        };

        // step_key is at index 8, may be null
        let step_key = if row.values.len() > 8 {
            match &row.values[8] {
                Cell::String(s) => Some(Arc::from(s.as_str())),
                Cell::Null => None,
                _ => None,
            }
        } else {
            None
        };

        // task_key is at index 9, may be null
        let task_key = if row.values.len() > 9 {
            match &row.values[9] {
                Cell::String(s) => Some(Arc::from(s.as_str())),
                Cell::Null => None,
                _ => None,
            }
        } else {
            None
        };

        match source {
            "db" => {
                let schema_name = match &row.values[2] {
                    Cell::String(s) => Arc::from(s.as_str()),
                    _ => return None,
                };
                let table_name = match &row.values[3] {
                    Cell::String(s) => Arc::from(s.as_str()),
                    _ => return None,
                };
                let op = match &row.values[4] {
                    Cell::String(s) => match s.as_str() {
                        "insert" => Operation::Insert,
                        "update" => Operation::Update,
                        "delete" => Operation::Delete,
                        _ => return None,
                    },
                    _ => return None,
                };

                // columns is at index 10
                let columns = if row.values.len() > 10 {
                    match &row.values[10] {
                        Cell::Array(ArrayCell::String(vec)) => {
                            // PostgreSQL text[] array as ArrayCell::String
                            let result: Vec<Arc<str>> = vec.iter()
                                .filter_map(|opt| opt.as_ref().map(|s| Arc::from(s.as_str())))
                                .collect();
                            if result.is_empty() && vec.is_empty() {
                                Some(vec![])
                            } else if result.is_empty() {
                                None
                            } else {
                                Some(result)
                            }
                        }
                        Cell::Json(v) => {
                            // Array of strings in JSON format
                            if let Some(arr) = v.as_array() {
                                Some(arr.iter()
                                    .filter_map(|v| v.as_str().map(|s| Arc::from(s)))
                                    .collect())
                            } else {
                                None
                            }
                        }
                        Cell::String(s) => {
                            // PostgreSQL text[] array format: {email,name}
                            let trimmed = s.trim();
                            if trimmed.starts_with('{') && trimmed.ends_with('}') {
                                let inner = &trimmed[1..trimmed.len()-1];
                                if inner.is_empty() {
                                    Some(vec![])
                                } else {
                                    Some(inner.split(',')
                                        .map(|s| Arc::from(s.trim()))
                                        .collect())
                                }
                            } else {
                                None
                            }
                        }
                        Cell::Null => None,
                        _ => None,
                    }
                } else {
                    None
                };

                Some(Subscription::Db(DbSubscription {
                    id,
                    schema_name,
                    table_name,
                    op,
                    execution_id,
                    queue,
                    step_key,
                    task_key,
                    columns,
                }))
            }
            "event" => {
                let event_key = match &row.values[5] {
                    Cell::String(s) => Arc::from(s.as_str()),
                    _ => return None,
                };

                Some(Subscription::Event(EventSubscription {
                    id,
                    event_key,
                    execution_id,
                    queue,
                    step_key,
                    task_key,
                }))
            }
            _ => None,
        }
    }

    /// Handle an insert event on the events table (custom event)
    fn handle_custom_event(&self, row: &TableRow) {
        // Expected: id, event_key, payload
        if row.values.len() < 3 {
            return;
        }

        let event_key = match &row.values[1] {
            Cell::String(s) => s.as_str(),
            _ => return,
        };

        let payload = match &row.values[2] {
            Cell::Json(v) => v.clone(),
            _ => serde_json::Value::Null,
        };

        if let Some(subs) = match_event_subs(event_key) {
            for sub in subs.iter() {
                // Check if this is a trigger-based or step-based subscription
                if let Some(task_key) = &sub.task_key {
                    // Trigger-based: invoke new execution
                    info!(
                        event_key = %event_key,
                        task_key = %task_key,
                        queue = %sub.queue,
                        "Invoking task from custom event trigger"
                    );
                    self.invoke_from_event(task_key, &sub.queue, event_key, payload.clone());
                } else if let Some(execution_id) = sub.execution_id {
                    // Step-based: wake existing execution
                    let step_key = sub.step_key.as_deref().unwrap_or("event");
                    info!(
                        event_key = %event_key,
                        execution_id = %execution_id,
                        queue = %sub.queue,
                        step_key = %step_key,
                        "Routing custom event to execution"
                    );
                    self.wake_execution(execution_id, &sub.queue, step_key, payload.clone());
                }
            }
        }
    }

    /// Handle a DB event (insert/update/delete on any table)
    fn handle_db_event(
        &self,
        schema: &str,
        table: &str,
        op: Operation,
        column_names: &[String],
        old_row: Option<&TableRow>,
        new_row: Option<&TableRow>,
    ) {
        if let Some(subs) = match_db_subs(schema, table, op) {
            let tg_op = match op {
                Operation::Insert => "INSERT",
                Operation::Update => "UPDATE",
                Operation::Delete => "DELETE",
            };

            let op_str = match op {
                Operation::Insert => "insert",
                Operation::Update => "update",
                Operation::Delete => "delete",
            };

            for sub in subs.iter() {
                // Build payload with filtered columns based on subscription
                info!(
                    sub_columns = ?sub.columns,
                    "Applying column filter for db event"
                );
                let old_json = old_row.map(|r| self.row_to_json_filtered(r, column_names, sub.columns.as_deref())).unwrap_or(serde_json::Value::Null);
                let new_json = new_row.map(|r| self.row_to_json_filtered(r, column_names, sub.columns.as_deref())).unwrap_or(serde_json::Value::Null);

                let payload = serde_json::json!({
                    "old": old_json,
                    "new": new_json,
                    "tg_table": table,
                    "tg_op": tg_op
                });

                // Check if this is a trigger-based or step-based subscription
                if let Some(task_key) = &sub.task_key {
                    // Trigger-based: invoke new execution
                    let event_name = format!("{}.{}.{}", schema, table, op_str);
                    info!(
                        schema = %schema,
                        table = %table,
                        op = ?op,
                        task_key = %task_key,
                        queue = %sub.queue,
                        "Invoking task from DB event trigger"
                    );
                    self.invoke_from_event(task_key, &sub.queue, &event_name, payload);
                } else if let Some(execution_id) = sub.execution_id {
                    // Step-based: wake existing execution
                    let step_key = sub.step_key.as_deref()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| format!("{}.{}.{:?}", schema, table, op));
                    info!(
                        schema = %schema,
                        table = %table,
                        op = ?op,
                        execution_id = %execution_id,
                        queue = %sub.queue,
                        step_key = %step_key,
                        "Routing DB event to execution"
                    );
                    self.wake_execution(execution_id, &sub.queue, &step_key, payload);
                }
            }
        }
    }

    /// Convert a TableRow to JSON object with column names, optionally filtered
    fn row_to_json_filtered(&self, row: &TableRow, column_names: &[String], filter_columns: Option<&[Arc<str>]>) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        for (i, cell) in row.values.iter().enumerate() {
            if let Some(col_name) = column_names.get(i) {
                // Check if we should include this column
                let include = match filter_columns {
                    Some(cols) => cols.iter().any(|c| c.as_ref() == col_name),
                    None => true, // No filter means include all
                };

                if include {
                    let value = match cell {
                        Cell::Null => serde_json::Value::Null,
                        Cell::Bool(b) => serde_json::Value::Bool(*b),
                        Cell::String(s) => serde_json::Value::String(s.clone()),
                        Cell::I16(n) => serde_json::json!(n),
                        Cell::I32(n) => serde_json::json!(n),
                        Cell::U32(n) => serde_json::json!(n),
                        Cell::I64(n) => serde_json::json!(n),
                        Cell::F32(n) => serde_json::json!(n),
                        Cell::F64(n) => serde_json::json!(n),
                        Cell::Json(v) => v.clone(),
                        Cell::Uuid(u) => serde_json::Value::String(u.to_string()),
                        Cell::Bytes(b) => serde_json::Value::String(base64_encode(b)),
                        _ => serde_json::Value::Null,
                    };
                    obj.insert(col_name.clone(), value);
                }
            }
        }

        serde_json::Value::Object(obj)
    }

    /// Wake up an execution by calling the SQL function
    fn wake_execution(&self, execution_id: Uuid, queue: &str, step_key: &str, payload: serde_json::Value) {
        let pool = self.pool.clone();
        let execution_id = execution_id;
        let queue = queue.to_string();
        let step_key = step_key.to_string();

        // Spawn a task to call the SQL function
        tokio::spawn(async move {
            let result = sqlx::query(
                "SELECT pgconductor.wake_execution($1, $2, $3, $4)"
            )
            .bind(execution_id)
            .bind(&queue)
            .bind(&step_key)
            .bind(&payload)
            .execute(&pool)
            .await;

            match result {
                Ok(_) => info!(execution_id = %execution_id, queue = %queue, step_key = %step_key, "Woke execution"),
                Err(e) => warn!(execution_id = %execution_id, error = %e, "Failed to wake execution"),
            }
        });
    }

    /// Invoke a new execution from an event trigger
    fn invoke_from_event(&self, task_key: &str, queue: &str, event_name: &str, payload: serde_json::Value) {
        let pool = self.pool.clone();
        let task_key = task_key.to_string();
        let queue = queue.to_string();
        let event_name = event_name.to_string();

        // Spawn a task to call the SQL function
        tokio::spawn(async move {
            let result = sqlx::query(
                "SELECT pgconductor.invoke_from_event($1, $2, $3, $4)"
            )
            .bind(&task_key)
            .bind(&queue)
            .bind(&event_name)
            .bind(&payload)
            .execute(&pool)
            .await;

            match result {
                Ok(_) => info!(task_key = %task_key, queue = %queue, event_name = %event_name, "Invoked task from event"),
                Err(e) => warn!(task_key = %task_key, error = %e, "Failed to invoke task from event"),
            }
        });
    }

    /// Get table name info from relation events
    fn register_table(&self, table_id: TableId, schema: String, name: String, columns: Vec<String>) {
        self.table_names.lock().insert(table_id, (schema, name, columns));
    }

    /// Look up table name and columns
    fn get_table_name(&self, table_id: TableId) -> Option<(String, String, Vec<String>)> {
        self.table_names.lock().get(&table_id).cloned()
    }
}

impl Destination for EventRouterDestination {
    fn name() -> &'static str {
        "event-router"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        // Handle truncate of subscriptions table - clear index
        if let Some((schema, name, _columns)) = self.get_table_name(table_id) {
            if schema == PGCONDUCTOR_SCHEMA && name == SUBSCRIPTIONS_TABLE {
                rebuild_index(Vec::new());
                info!("Subscriptions table truncated, index cleared");
            }
        }
        Ok(())
    }

    async fn write_table_rows(
        &self,
        _table_id: TableId,
        _table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        // We load subscriptions from DB on startup, not from initial sync
        // This handles the case where the app restarts and replication continues
        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        for event in events {
            match event {
                Event::Relation(rel) => {
                    // Register table name mapping with column names
                    let table_id = rel.table_schema.id;
                    let schema = rel.table_schema.name.schema.clone();
                    let name = rel.table_schema.name.name.clone();
                    let columns: Vec<String> = rel.table_schema.column_schemas.iter()
                        .map(|c| c.name.clone())
                        .collect();
                    debug!(table_id = ?table_id, schema = %schema, name = %name, columns = ?columns, "Registered table");
                    self.register_table(table_id, schema, name, columns);
                }
                Event::Insert(insert) => {
                    if let Some((schema, name, columns)) = self.get_table_name(insert.table_id) {
                        debug!(table_id = ?insert.table_id, schema = %schema, name = %name, "Received INSERT event");
                        // Handle subscriptions table changes
                        if schema == PGCONDUCTOR_SCHEMA && name == SUBSCRIPTIONS_TABLE {
                            debug!("Processing subscriptions INSERT");
                            if let Some(sub) = self.parse_subscription_row(&insert.table_row) {
                                info!("Adding subscription from CDC");
                                self.add_subscription(sub);
                            } else {
                                warn!("Failed to parse subscription row from CDC");
                            }
                        }
                        // Handle events table (custom events)
                        else if schema == PGCONDUCTOR_SCHEMA && name == EVENTS_TABLE {
                            self.handle_custom_event(&insert.table_row);
                        }
                        // Handle any other table for DB event subscriptions
                        else {
                            self.handle_db_event(&schema, &name, Operation::Insert, &columns, None, Some(&insert.table_row));
                        }
                    }
                }
                Event::Update(update) => {
                    if let Some((schema, name, columns)) = self.get_table_name(update.table_id) {
                        if schema == PGCONDUCTOR_SCHEMA && name == SUBSCRIPTIONS_TABLE {
                            // For simplicity, remove old and add new
                            if let Some((_, old_row)) = &update.old_table_row {
                                if let Some(old_sub) = self.parse_subscription_row(old_row) {
                                    self.remove_subscription_by_id(match &old_sub {
                                        Subscription::Db(s) => s.id,
                                        Subscription::Event(s) => s.id,
                                    });
                                }
                            }
                            if let Some(sub) = self.parse_subscription_row(&update.table_row) {
                                self.add_subscription(sub);
                            }
                        } else {
                            let old = update.old_table_row.as_ref().map(|(_, r)| r);
                            self.handle_db_event(&schema, &name, Operation::Update, &columns, old, Some(&update.table_row));
                        }
                    }
                }
                Event::Delete(delete) => {
                    if let Some((schema, name, columns)) = self.get_table_name(delete.table_id) {
                        if schema == PGCONDUCTOR_SCHEMA && name == SUBSCRIPTIONS_TABLE {
                            if let Some((_, old_row)) = &delete.old_table_row {
                                if let Some(sub) = self.parse_subscription_row(old_row) {
                                    self.remove_subscription_by_id(match &sub {
                                        Subscription::Db(s) => s.id,
                                        Subscription::Event(s) => s.id,
                                    });
                                }
                            }
                        } else if let Some((_, old_row)) = &delete.old_table_row {
                            self.handle_db_event(&schema, &name, Operation::Delete, &columns, Some(old_row), None);
                        }
                    }
                }
                _ => {} // Ignore Begin, Commit, Truncate handled above
            }
        }
        Ok(())
    }
}

impl EventRouterDestination {
    /// Add a subscription to the index
    fn add_subscription(&self, sub: Subscription) {
        // For efficiency, we rebuild the entire index
        // In production, you'd want incremental updates
        let current = SUBSCRIPTIONS.load();
        let mut all_subs: Vec<Subscription> = if let Some(idx) = current.as_deref() {
            idx.by_id.values().cloned().collect()
        } else {
            Vec::new()
        };
        all_subs.push(sub);
        rebuild_index(all_subs);
    }

    /// Remove a subscription by ID
    fn remove_subscription_by_id(&self, id: Uuid) {
        let current = SUBSCRIPTIONS.load();
        let all_subs: Vec<Subscription> = if let Some(idx) = current.as_deref() {
            idx.by_id
                .iter()
                .filter(|(sub_id, _)| **sub_id != id)
                .map(|(_, sub)| sub.clone())
                .collect()
        } else {
            Vec::new()
        };
        rebuild_index(all_subs);
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 4 / 3 + 4);
    for byte in bytes {
        write!(s, "{:02x}", byte).unwrap();
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl::types::Cell;
    use serde_json::json;

    fn create_test_destination() -> EventRouterDestination {
        // Create a minimal destination for testing
        // We only need the table_names field for row_to_json_filtered tests
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://test:test@localhost/test")
            .unwrap();
        EventRouterDestination {
            pool,
            initialized: Arc::new(AtomicBool::new(false)),
            table_names: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[tokio::test]
    async fn test_row_to_json_filtered_no_filter() {
        let dest = create_test_destination();
        let row = TableRow {
            values: vec![
                Cell::String("123".to_string()),
                Cell::String("john@example.com".to_string()),
                Cell::String("John".to_string()),
            ],
        };
        let column_names = vec![
            "id".to_string(),
            "email".to_string(),
            "first_name".to_string(),
        ];

        let result = dest.row_to_json_filtered(&row, &column_names, None);

        assert_eq!(result, json!({
            "id": "123",
            "email": "john@example.com",
            "first_name": "John"
        }));
    }

    #[tokio::test]
    async fn test_row_to_json_filtered_with_filter() {
        let dest = create_test_destination();
        let row = TableRow {
            values: vec![
                Cell::String("123".to_string()),
                Cell::String("john@example.com".to_string()),
                Cell::String("John".to_string()),
                Cell::String("Doe".to_string()),
                Cell::String("555-1234".to_string()),
            ],
        };
        let column_names = vec![
            "id".to_string(),
            "email".to_string(),
            "first_name".to_string(),
            "last_name".to_string(),
            "phone".to_string(),
        ];
        let filter: Vec<Arc<str>> = vec![
            Arc::from("id"),
            Arc::from("email"),
        ];

        let result = dest.row_to_json_filtered(&row, &column_names, Some(&filter));

        // Should only include id and email
        assert_eq!(result, json!({
            "id": "123",
            "email": "john@example.com"
        }));

        // Verify excluded columns are not present
        let obj = result.as_object().unwrap();
        assert!(!obj.contains_key("first_name"));
        assert!(!obj.contains_key("last_name"));
        assert!(!obj.contains_key("phone"));
    }

    #[tokio::test]
    async fn test_row_to_json_filtered_single_column() {
        let dest = create_test_destination();
        let row = TableRow {
            values: vec![
                Cell::String("123".to_string()),
                Cell::String("john@example.com".to_string()),
                Cell::String("John".to_string()),
            ],
        };
        let column_names = vec![
            "id".to_string(),
            "email".to_string(),
            "first_name".to_string(),
        ];
        let filter: Vec<Arc<str>> = vec![Arc::from("email")];

        let result = dest.row_to_json_filtered(&row, &column_names, Some(&filter));

        assert_eq!(result, json!({
            "email": "john@example.com"
        }));
    }

    #[tokio::test]
    async fn test_row_to_json_filtered_various_types() {
        let dest = create_test_destination();
        let row = TableRow {
            values: vec![
                Cell::I32(42),
                Cell::Bool(true),
                Cell::Null,
                Cell::F64(3.14),
            ],
        };
        let column_names = vec![
            "count".to_string(),
            "active".to_string(),
            "deleted_at".to_string(),
            "score".to_string(),
        ];
        let filter: Vec<Arc<str>> = vec![
            Arc::from("count"),
            Arc::from("active"),
        ];

        let result = dest.row_to_json_filtered(&row, &column_names, Some(&filter));

        assert_eq!(result, json!({
            "count": 42,
            "active": true
        }));
    }

    #[tokio::test]
    async fn test_row_to_json_filtered_empty_filter() {
        let dest = create_test_destination();
        let row = TableRow {
            values: vec![
                Cell::String("123".to_string()),
                Cell::String("john@example.com".to_string()),
            ],
        };
        let column_names = vec![
            "id".to_string(),
            "email".to_string(),
        ];
        let filter: Vec<Arc<str>> = vec![];

        let result = dest.row_to_json_filtered(&row, &column_names, Some(&filter));

        // Empty filter should include nothing
        assert_eq!(result, json!({}));
    }

    #[tokio::test]
    async fn test_row_to_json_filtered_nonexistent_column() {
        let dest = create_test_destination();
        let row = TableRow {
            values: vec![
                Cell::String("123".to_string()),
                Cell::String("john@example.com".to_string()),
            ],
        };
        let column_names = vec![
            "id".to_string(),
            "email".to_string(),
        ];
        let filter: Vec<Arc<str>> = vec![
            Arc::from("id"),
            Arc::from("nonexistent"),
        ];

        let result = dest.row_to_json_filtered(&row, &column_names, Some(&filter));

        // Should only include id since nonexistent doesn't exist
        assert_eq!(result, json!({
            "id": "123"
        }));
    }

    #[tokio::test]
    async fn test_parse_subscription_row_with_array_cell_columns() {
        let dest = create_test_destination();

        // Create a db subscription row with columns as ArrayCell::String (actual CDC format)
        let row = TableRow {
            values: vec![
                Cell::String("550e8400-e29b-41d4-a716-446655440000".to_string()), // id
                Cell::String("db".to_string()),                                    // source
                Cell::String("public".to_string()),                                // schema_name
                Cell::String("contact".to_string()),                               // table_name
                Cell::String("update".to_string()),                                // operation
                Cell::Null,                                                        // event_key (null for db)
                Cell::String("660e8400-e29b-41d4-a716-446655440001".to_string()), // execution_id
                Cell::String("default".to_string()),                               // queue
                Cell::String("wait-step".to_string()),                             // step_key
                Cell::Null,                                                        // task_key
                Cell::Array(ArrayCell::String(vec![
                    Some("email".to_string()),
                    Some("name".to_string()),
                ])),                                                               // columns as ArrayCell
            ],
        };

        let sub = dest.parse_subscription_row(&row);
        assert!(sub.is_some(), "Should parse subscription row");

        if let Some(Subscription::Db(db_sub)) = sub {
            assert_eq!(db_sub.schema_name.as_ref(), "public");
            assert_eq!(db_sub.table_name.as_ref(), "contact");
            assert_eq!(db_sub.op, Operation::Update);
            assert!(db_sub.columns.is_some(), "Columns should be parsed");

            let columns = db_sub.columns.unwrap();
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].as_ref(), "email");
            assert_eq!(columns[1].as_ref(), "name");
        } else {
            panic!("Expected Db subscription");
        }
    }

    #[tokio::test]
    async fn test_parse_subscription_row_with_empty_array_cell() {
        let dest = create_test_destination();

        let row = TableRow {
            values: vec![
                Cell::String("550e8400-e29b-41d4-a716-446655440000".to_string()),
                Cell::String("db".to_string()),
                Cell::String("public".to_string()),
                Cell::String("contact".to_string()),
                Cell::String("insert".to_string()),
                Cell::Null,
                Cell::String("660e8400-e29b-41d4-a716-446655440001".to_string()),
                Cell::String("default".to_string()),
                Cell::Null,
                Cell::Null,
                Cell::Array(ArrayCell::String(vec![])), // empty array
            ],
        };

        let sub = dest.parse_subscription_row(&row);
        assert!(sub.is_some());

        if let Some(Subscription::Db(db_sub)) = sub {
            assert!(db_sub.columns.is_some());
            assert_eq!(db_sub.columns.unwrap().len(), 0);
        } else {
            panic!("Expected Db subscription");
        }
    }
}
