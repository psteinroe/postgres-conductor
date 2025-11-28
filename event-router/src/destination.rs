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
const SUBSCRIPTIONS_TABLE: &str = "_private_subscriptions";
const EVENTS_TABLE: &str = "_private_events";
const PGCONDUCTOR_SCHEMA: &str = "pgconductor";

/// Event router destination that:
/// 1. Maintains in-memory index of subscriptions via CDC
/// 2. Routes events from pgconductor._private_events to waiting executions or triggers
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
            FROM pgconductor._private_subscriptions
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
          let op = match operation.to_lowercase().as_str() {
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
          Cell::String(s) => match s.to_lowercase().as_str() {
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
              let result: Vec<Arc<str>> =
                vec.iter().filter_map(|opt| opt.as_ref().map(|s| Arc::from(s.as_str()))).collect();
              if result.is_empty() && vec.is_empty() {
                Some(vec![])
              } else if result.is_empty() {
                None
              } else {
                Some(result)
              }
            }
            Cell::Json(v) => {
              if let Some(arr) = v.as_array() {
                Some(arr.iter().filter_map(|v| v.as_str().map(|s| Arc::from(s))).collect())
              } else {
                None
              }
            }
            Cell::String(s) => {
              let trimmed = s.trim();
              if trimmed.starts_with('{') && trimmed.ends_with('}') {
                let inner = &trimmed[1..trimmed.len() - 1];
                if inner.is_empty() {
                  Some(vec![])
                } else {
                  Some(inner.split(',').map(|s| Arc::from(s.trim())).collect())
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

  /// Handle an event from the pgconductor._private_events table
  fn handle_event(&self, row: &TableRow) {
    // Expected columns: id, event_key, schema_name, table_name, operation, source, payload, created_at
    if row.values.len() < 7 {
      warn!(num_columns = row.values.len(), "Invalid event row: not enough columns");
      return;
    }

    let event_key = match &row.values[1] {
      Cell::String(s) => s.as_str(),
      other => {
        warn!(cell_type = ?other, "event_key is not a string");
        return;
      }
    };

    let source = match &row.values[5] {
      Cell::String(s) => s.as_str(),
      other => {
        warn!(cell_type = ?other, "source is not a string");
        return;
      }
    };

    let payload = match &row.values[6] {
      Cell::Json(v) => v.clone(),
      Cell::Null => serde_json::Value::Null,
      _ => serde_json::Value::Null,
    };

    debug!(event_key = %event_key, source = %source, "Processing event");

    match source {
      "db" => {
        // Database event - look up by schema/table/operation
        let schema_name = match &row.values[2] {
          Cell::String(s) => s.as_str(),
          _ => return,
        };
        let table_name = match &row.values[3] {
          Cell::String(s) => s.as_str(),
          _ => return,
        };
        let operation = match &row.values[4] {
          Cell::String(s) => s.as_str(),
          _ => return,
        };

        let op = match operation.to_uppercase().as_str() {
          "INSERT" => Operation::Insert,
          "UPDATE" => Operation::Update,
          "DELETE" => Operation::Delete,
          _ => return,
        };

        if let Some(subs) = match_db_subs(schema_name, table_name, op) {
          for sub in subs.iter() {
            // Filter payload columns if subscription has column filter
            let filtered_payload = if let Some(filter_cols) = &sub.columns {
              self.filter_payload_columns(&payload, filter_cols)
            } else {
              payload.clone()
            };

            if let Some(task_key) = &sub.task_key {
              // Trigger-based: invoke new execution
              info!(
                  event_key = %event_key,
                  task_key = %task_key,
                  queue = %sub.queue,
                  "Invoking task from DB event trigger"
              );
              self.invoke_from_event(task_key, &sub.queue, event_key, filtered_payload);
            } else if let Some(execution_id) = sub.execution_id {
              // Step-based: wake existing execution
              let step_key = sub.step_key.as_deref().unwrap_or("db_event");
              info!(
                  event_key = %event_key,
                  execution_id = %execution_id,
                  queue = %sub.queue,
                  step_key = %step_key,
                  "Routing DB event to execution"
              );
              self.wake_execution(execution_id, &sub.queue, step_key, filtered_payload);
            }
          }
        }
      }
      "event" => {
        // Custom event - look up by event_key
        if let Some(subs) = match_event_subs(event_key) {
          debug!(event_key = %event_key, num_subs = subs.len(), "Found matching event subscriptions");
          for sub in subs.iter() {
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
        } else {
          debug!(event_key = %event_key, "No matching subscriptions for event");
        }
      }
      _ => {
        warn!(source = %source, "Unknown event source");
      }
    }
  }

  /// Filter payload columns based on subscription column filter
  fn filter_payload_columns(
    &self,
    payload: &serde_json::Value,
    filter_cols: &[Arc<str>],
  ) -> serde_json::Value {
    let Some(obj) = payload.as_object() else {
      return payload.clone();
    };

    let mut result = serde_json::Map::new();

    // Preserve tg_op if present
    if let Some(tg_op) = obj.get("tg_op") {
      result.insert("tg_op".to_string(), tg_op.clone());
    }

    // Filter 'old' and 'new' objects if present
    if let Some(old) = obj.get("old").and_then(|v| v.as_object()) {
      let filtered: serde_json::Map<String, serde_json::Value> = old
        .iter()
        .filter(|(k, _)| filter_cols.iter().any(|c| c.as_ref() == k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
      result.insert("old".to_string(), serde_json::Value::Object(filtered));
    } else if obj.contains_key("old") {
      result.insert("old".to_string(), serde_json::Value::Null);
    }

    if let Some(new) = obj.get("new").and_then(|v| v.as_object()) {
      let filtered: serde_json::Map<String, serde_json::Value> = new
        .iter()
        .filter(|(k, _)| filter_cols.iter().any(|c| c.as_ref() == k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
      result.insert("new".to_string(), serde_json::Value::Object(filtered));
    } else if obj.contains_key("new") {
      result.insert("new".to_string(), serde_json::Value::Null);
    }

    serde_json::Value::Object(result)
  }

  /// Wake up an execution by calling the SQL function
  fn wake_execution(
    &self,
    execution_id: Uuid,
    queue: &str,
    step_key: &str,
    payload: serde_json::Value,
  ) {
    let pool = self.pool.clone();
    let execution_id = execution_id;
    let queue = queue.to_string();
    let step_key = step_key.to_string();

    tokio::spawn(async move {
      // Wake execution: insert step and clear waiting state
      // Note: data-modifying CTEs execute regardless of whether the main query uses them
      let result = sqlx::query(
        "WITH step_insert AS (
          INSERT INTO pgconductor._private_steps (execution_id, queue, key, result)
          VALUES ($1, $2, $3, $4)
          ON CONFLICT (execution_id, key) DO NOTHING
        )
        UPDATE pgconductor._private_executions
        SET
          waiting_on_execution_id = NULL,
          waiting_step_key = NULL,
          run_at = pgconductor._private_current_time()
        WHERE id = $1 AND queue = $2",
      )
      .bind(execution_id)
      .bind(&queue)
      .bind(&step_key)
      .bind(&payload)
      .execute(&pool)
      .await;

      match result {
        Ok(_) => {
          info!(execution_id = %execution_id, queue = %queue, step_key = %step_key, "Woke execution")
        }
        Err(e) => warn!(execution_id = %execution_id, error = %e, "Failed to wake execution"),
      }
    });
  }

  /// Invoke a new execution from an event trigger
  fn invoke_from_event(
    &self,
    task_key: &str,
    queue: &str,
    event_name: &str,
    payload: serde_json::Value,
  ) {
    let pool = self.pool.clone();
    let task_key = task_key.to_string();
    let queue = queue.to_string();
    let event_name = event_name.to_string();

    tokio::spawn(async move {
      let result = sqlx::query("SELECT pgconductor._private_invoke_from_event($1, $2, $3, $4)")
        .bind(&task_key)
        .bind(&queue)
        .bind(&event_name)
        .bind(&payload)
        .execute(&pool)
        .await;

      match result {
        Ok(_) => {
          info!(task_key = %task_key, queue = %queue, event_name = %event_name, "Invoked task from event")
        }
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
    Ok(())
  }

  async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
    for event in events {
      match event {
        Event::Relation(rel) => {
          let table_id = rel.table_schema.id;
          let schema = rel.table_schema.name.schema.clone();
          let name = rel.table_schema.name.name.clone();
          let columns: Vec<String> =
            rel.table_schema.column_schemas.iter().map(|c| c.name.clone()).collect();
          debug!(table_id = ?table_id, schema = %schema, name = %name, "Registered table");
          self.register_table(table_id, schema, name, columns);
        }
        Event::Insert(insert) => {
          if let Some((schema, name, _columns)) = self.get_table_name(insert.table_id) {
            // Only handle pgconductor schema tables
            if schema == PGCONDUCTOR_SCHEMA {
              if name == SUBSCRIPTIONS_TABLE {
                // Subscription added
                debug!("Received INSERT on subscriptions table");
                if let Some(sub) = self.parse_subscription_row(&insert.table_row) {
                  info!("Adding subscription from CDC");
                  self.add_subscription(sub);
                } else {
                  warn!("Failed to parse subscription row");
                }
              } else if name == EVENTS_TABLE {
                // Event published - route to subscribers
                debug!("Received INSERT on events table");
                self.handle_event(&insert.table_row);
              }
            }
          } else {
            debug!(table_id = ?insert.table_id, "INSERT on unknown table (no relation event received)");
          }
        }
        Event::Update(update) => {
          if let Some((schema, name, _columns)) = self.get_table_name(update.table_id) {
            if schema == PGCONDUCTOR_SCHEMA && name == SUBSCRIPTIONS_TABLE {
              // Subscription updated - remove old and add new
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
            }
          }
        }
        Event::Delete(delete) => {
          if let Some((schema, name, _columns)) = self.get_table_name(delete.table_id) {
            if schema == PGCONDUCTOR_SCHEMA && name == SUBSCRIPTIONS_TABLE {
              // Subscription removed
              if let Some((_, old_row)) = &delete.old_table_row {
                if let Some(sub) = self.parse_subscription_row(old_row) {
                  self.remove_subscription_by_id(match &sub {
                    Subscription::Db(s) => s.id,
                    Subscription::Event(s) => s.id,
                  });
                }
              }
            }
          }
        }
        _ => {}
      }
    }
    Ok(())
  }
}

impl EventRouterDestination {
  /// Add a subscription to the index
  fn add_subscription(&self, sub: Subscription) {
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
      idx.by_id.iter().filter(|(sub_id, _)| **sub_id != id).map(|(_, sub)| sub.clone()).collect()
    } else {
      Vec::new()
    };
    rebuild_index(all_subs);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  fn create_test_destination() -> EventRouterDestination {
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
  async fn test_filter_payload_columns() {
    let dest = create_test_destination();

    let payload = json!({
        "old": {
            "id": "123",
            "email": "old@example.com",
            "first_name": "John",
            "phone": "555-1234"
        },
        "new": {
            "id": "123",
            "email": "new@example.com",
            "first_name": "Jane",
            "phone": "555-5678"
        }
    });

    let filter: Vec<Arc<str>> = vec![Arc::from("id"), Arc::from("email")];

    let result = dest.filter_payload_columns(&payload, &filter);

    assert_eq!(
      result,
      json!({
          "old": {
              "id": "123",
              "email": "old@example.com"
          },
          "new": {
              "id": "123",
              "email": "new@example.com"
          }
      })
    );
  }

  #[tokio::test]
  async fn test_filter_payload_columns_with_null() {
    let dest = create_test_destination();

    let payload = json!({
        "old": null,
        "new": {
            "id": "123",
            "email": "new@example.com"
        }
    });

    let filter: Vec<Arc<str>> = vec![Arc::from("email")];

    let result = dest.filter_payload_columns(&payload, &filter);

    assert_eq!(
      result,
      json!({
          "old": null,
          "new": {
              "email": "new@example.com"
          }
      })
    );
  }

  #[tokio::test]
  async fn test_parse_subscription_row_with_array_cell_columns() {
    let dest = create_test_destination();

    let row = TableRow {
      values: vec![
        Cell::String("550e8400-e29b-41d4-a716-446655440000".to_string()),
        Cell::String("db".to_string()),
        Cell::String("public".to_string()),
        Cell::String("contact".to_string()),
        Cell::String("update".to_string()),
        Cell::Null,
        Cell::String("660e8400-e29b-41d4-a716-446655440001".to_string()),
        Cell::String("default".to_string()),
        Cell::String("wait-step".to_string()),
        Cell::Null,
        Cell::Array(ArrayCell::String(vec![Some("email".to_string()), Some("name".to_string())])),
      ],
    };

    let sub = dest.parse_subscription_row(&row);
    assert!(sub.is_some());

    if let Some(Subscription::Db(db_sub)) = sub {
      assert_eq!(db_sub.schema_name.as_ref(), "public");
      assert_eq!(db_sub.table_name.as_ref(), "contact");
      assert_eq!(db_sub.op, Operation::Update);
      assert!(db_sub.columns.is_some());

      let columns = db_sub.columns.unwrap();
      assert_eq!(columns.len(), 2);
      assert_eq!(columns[0].as_ref(), "email");
      assert_eq!(columns[1].as_ref(), "name");
    } else {
      panic!("Expected Db subscription");
    }
  }
}
