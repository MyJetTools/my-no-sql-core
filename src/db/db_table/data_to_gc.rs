use std::{collections::BTreeMap, sync::Arc};

use crate::db::DbRow;

pub struct DataToExpireInner {
    pub partitions: BTreeMap<String, ()>,
    pub db_rows: BTreeMap<String, Vec<Arc<DbRow>>>,
}

pub struct DataToGc {
    inner: Option<DataToExpireInner>,
}

impl DataToGc {
    pub fn new() -> Self {
        Self { inner: None }
    }

    fn get_inner(&mut self) -> &mut DataToExpireInner {
        if self.inner.is_none() {
            self.inner = Some(DataToExpireInner {
                partitions: BTreeMap::new(),
                db_rows: BTreeMap::new(),
            });
        }
        self.inner.as_mut().unwrap()
    }

    pub fn add_partition_to_expire(&mut self, partition_key: &str) {
        let inner = self.get_inner();

        if inner.partitions.contains_key(partition_key) {
            return;
        }

        inner.partitions.insert(partition_key.to_string(), ());
    }

    pub fn add_rows_to_expire(&mut self, partition_key: &str, rows: Vec<Arc<DbRow>>) {
        let inner = self.get_inner();

        if inner.partitions.contains_key(partition_key) {
            return;
        }

        if !inner.db_rows.contains_key(partition_key) {
            inner.db_rows.insert(partition_key.to_string(), rows);
            return;
        }

        inner.db_rows.get_mut(partition_key).unwrap().extend(rows);
    }

    pub fn has_partition_to_gc(&self, partition_key: &str) -> bool {
        if let Some(inner) = self.inner.as_ref() {
            inner.partitions.contains_key(partition_key)
        } else {
            false
        }
    }

    pub fn has_data_to_gc(&self) -> bool {
        self.inner.is_some()
    }

    pub fn get_partitions_to_gc(&self) -> Option<&BTreeMap<String, ()>> {
        let inner = self.inner.as_ref()?;
        Some(&inner.partitions)
    }

    pub fn get_rows_to_gc(&self) -> Option<&BTreeMap<String, Vec<Arc<DbRow>>>> {
        let inner = self.inner.as_ref()?;
        Some(&inner.db_rows)
    }
}
