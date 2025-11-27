use crate::models::{DbSubscription, EventSubscription, Operation, Subscription};
use arc_swap::ArcSwapOption;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Key for DB subscriptions: (schema, table, operation)
pub type DbKey = (Arc<str>, Arc<str>, Operation);

/// Index for fast subscription lookups
#[derive(Default)]
pub struct SubscriptionIndex {
  /// DB event subscriptions by (schema, table, operation)
  pub db_subs: HashMap<DbKey, Arc<[Arc<DbSubscription>]>>,
  /// Custom event subscriptions by event_key
  pub event_subs: HashMap<Arc<str>, Arc<[Arc<EventSubscription>]>>,
  /// All subscriptions by id for updates/deletes
  pub by_id: HashMap<Uuid, Subscription>,
}

pub static SUBSCRIPTIONS: ArcSwapOption<SubscriptionIndex> = ArcSwapOption::const_empty();

/// Clear the subscription index
pub fn clear_index() {
  SUBSCRIPTIONS.store(None);
}

/// Rebuild the entire index from a list of subscriptions
pub fn rebuild_index(subs: Vec<Subscription>) {
  let mut db_map: HashMap<DbKey, Vec<Arc<DbSubscription>>> = HashMap::new();
  let mut event_map: HashMap<Arc<str>, Vec<Arc<EventSubscription>>> = HashMap::new();
  let mut by_id: HashMap<Uuid, Subscription> = HashMap::new();

  for sub in subs {
    match &sub {
      Subscription::Db(db_sub) => {
        let key = (db_sub.schema_name.clone(), db_sub.table_name.clone(), db_sub.op);
        db_map.entry(key).or_default().push(Arc::new(db_sub.clone()));
        by_id.insert(db_sub.id, sub);
      }
      Subscription::Event(event_sub) => {
        event_map.entry(event_sub.event_key.clone()).or_default().push(Arc::new(event_sub.clone()));
        by_id.insert(event_sub.id, sub);
      }
    }
  }

  let db_subs = db_map.into_iter().map(|(k, v)| (k, Arc::from(v.into_boxed_slice()))).collect();

  let event_subs =
    event_map.into_iter().map(|(k, v)| (k, Arc::from(v.into_boxed_slice()))).collect();

  SUBSCRIPTIONS.store(Some(Arc::new(SubscriptionIndex { db_subs, event_subs, by_id })));
}

/// Find DB subscriptions matching schema.table + operation
pub fn match_db_subs(
  schema: &str,
  table: &str,
  op: Operation,
) -> Option<Arc<[Arc<DbSubscription>]>> {
  let guard = SUBSCRIPTIONS.load();
  let idx = guard.as_deref()?;
  idx.db_subs.get(&(Arc::from(schema), Arc::from(table), op)).cloned()
}

/// Find event subscriptions matching an event key
pub fn match_event_subs(event_key: &str) -> Option<Arc<[Arc<EventSubscription>]>> {
  let guard = SUBSCRIPTIONS.load();
  let idx = guard.as_deref()?;
  idx.event_subs.get(&Arc::from(event_key) as &Arc<str>).cloned()
}
