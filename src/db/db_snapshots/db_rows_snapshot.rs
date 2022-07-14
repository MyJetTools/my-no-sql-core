use std::sync::Arc;

use my_json::json_writer::JsonArrayWriter;

use crate::db::DbRow;

pub struct DbRowsSnapshot {
    pub db_rows: Vec<Arc<DbRow>>,
}

impl DbRowsSnapshot {
    pub fn new() -> Self {
        Self {
            db_rows: Vec::new(),
        }
    }

    pub fn new_from_snapshot(db_rows: Vec<Arc<DbRow>>) -> Self {
        Self { db_rows }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            db_rows: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, db_row: Arc<DbRow>) {
        self.db_rows.push(db_row);
    }

    pub fn len(&self) -> usize {
        self.db_rows.len()
    }

    pub fn as_json_array(&self) -> JsonArrayWriter {
        let mut json_array_writer = JsonArrayWriter::new();
        for db_row in &self.db_rows {
            json_array_writer.write_raw_element(db_row.data.as_slice());
        }

        json_array_writer
    }
}
