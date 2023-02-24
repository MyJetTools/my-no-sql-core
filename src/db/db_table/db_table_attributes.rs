use rust_extensions::date_time::DateTimeAsMicroseconds;

#[derive(Debug, Clone)]
pub struct DbTableAttributes {
    pub persist: bool,
    pub max_partitions_amount: Option<usize>,
    pub max_rows_per_partition_amount: Option<usize>,
    pub created: DateTimeAsMicroseconds,
}

impl DbTableAttributes {
    pub fn create_default() -> Self {
        Self {
            created: DateTimeAsMicroseconds::now(),
            persist: true,
            max_partitions_amount: None,
            max_rows_per_partition_amount: None,
        }
    }
}

impl DbTableAttributes {
    pub fn new(
        persist: bool,
        max_partitions_amount: Option<usize>,
        max_rows_per_partition_amount: Option<usize>,
        created: DateTimeAsMicroseconds,
    ) -> Self {
        Self {
            persist,
            created,
            max_partitions_amount,
            max_rows_per_partition_amount,
        }
    }

    pub fn update(
        &mut self,
        persist_table: bool,
        max_partitions_amount: Option<usize>,
        max_rows_per_partition_amount: Option<usize>,
    ) -> bool {
        let mut result = false;

        if self.persist != persist_table {
            self.persist = persist_table;
            result = true;
        }

        if self.max_partitions_amount != max_partitions_amount {
            self.max_partitions_amount = max_partitions_amount;
            result = true;
        }

        if self.max_rows_per_partition_amount != max_rows_per_partition_amount {
            self.max_rows_per_partition_amount = max_rows_per_partition_amount;
            result = true;
        }

        return result;
    }
}
