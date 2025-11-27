use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
  Insert,
  Update,
  Delete,
}

/// A subscription for DB table events (CDC)
#[derive(Debug, Clone)]
pub struct DbSubscription {
  pub id: Uuid,
  pub schema_name: Arc<str>,
  pub table_name: Arc<str>,
  pub op: Operation,
  /// For step-based subscriptions (waitForDbChange)
  pub execution_id: Option<Uuid>,
  pub queue: Arc<str>,
  pub step_key: Option<Arc<str>>,
  /// For trigger-based subscriptions (persistent)
  pub task_key: Option<Arc<str>>,
  pub columns: Option<Vec<Arc<str>>>,
}

/// A subscription for custom events by key
#[derive(Debug, Clone)]
pub struct EventSubscription {
  pub id: Uuid,
  pub event_key: Arc<str>,
  /// For step-based subscriptions (waitForEvent)
  pub execution_id: Option<Uuid>,
  pub queue: Arc<str>,
  pub step_key: Option<Arc<str>>,
  /// For trigger-based subscriptions (persistent)
  pub task_key: Option<Arc<str>>,
}

/// Union type for all subscription kinds
#[derive(Debug, Clone)]
pub enum Subscription {
  Db(DbSubscription),
  Event(EventSubscription),
}
