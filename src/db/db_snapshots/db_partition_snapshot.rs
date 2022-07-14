use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::db::db_snapshots::DbRowsSnapshot;

pub struct DbPartitionSnapshot {
    pub last_read_moment: DateTimeAsMicroseconds,
    pub last_write_moment: DateTimeAsMicroseconds,
    pub db_rows_snapshot: DbRowsSnapshot,
}

impl DbPartitionSnapshot {
    pub fn has_to_persist(&self, written_in_blob: DateTimeAsMicroseconds) -> bool {
        written_in_blob.unix_microseconds < self.last_write_moment.unix_microseconds
    }
}
