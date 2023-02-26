use std::collections::HashMap;

pub struct DataToGceInner {
    pub partitions: HashMap<String, ()>,
    pub db_rows: HashMap<String, Vec<String>>,
}

pub struct DataToGc {
    inner: Option<DataToGceInner>,
}

impl DataToGc {
    pub fn new() -> Self {
        Self { inner: None }
    }

    fn get_inner(&mut self) -> &mut DataToGceInner {
        if self.inner.is_none() {
            self.inner = Some(DataToGceInner {
                partitions: HashMap::new(),
                db_rows: HashMap::new(),
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

    pub fn add_rows_to_expire<'s, TRows: Iterator<Item = String>>(
        &mut self,
        partition_key: &str,
        rows: TRows,
    ) {
        let inner = self.get_inner();

        if inner.partitions.contains_key(partition_key) {
            return;
        }

        if !inner.db_rows.contains_key(partition_key) {
            inner
                .db_rows
                .insert(partition_key.to_string(), rows.collect());
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

    pub fn get_partitions_to_gc(&self) -> Option<&HashMap<String, ()>> {
        let inner = self.inner.as_ref()?;
        Some(&inner.partitions)
    }

    pub fn get_rows_to_gc(&self) -> Option<&HashMap<String, Vec<String>>> {
        let inner = self.inner.as_ref()?;
        Some(&inner.db_rows)
    }

    pub fn get_data_to_gc(self) -> Option<DataToGceInner> {
        self.inner
    }
}
