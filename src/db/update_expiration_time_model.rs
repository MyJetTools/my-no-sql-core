use rust_extensions::date_time::DateTimeAsMicroseconds;

pub enum UpdateExpirationDateTime {
    Yes(Option<DateTimeAsMicroseconds>),
    No,
}

pub struct UpdateExpirationTimeModel {
    pub update_db_rows_expiration_time: UpdateExpirationDateTime,
    pub update_db_partition_expiration_time: UpdateExpirationDateTime,
}

impl UpdateExpirationTimeModel {
    pub fn new(db_rows: Option<&String>, db_partition: Option<&String>) -> Option<Self> {
        if db_rows.is_none() && db_partition.is_none() {
            return None;
        }

        Self {
            update_db_rows_expiration_time: parse_date_time(db_rows),
            update_db_partition_expiration_time: parse_date_time(db_partition),
        }
        .into()
    }
}

fn parse_date_time(src: Option<&String>) -> UpdateExpirationDateTime {
    if let Some(src) = src {
        return UpdateExpirationDateTime::Yes(my_json::json_reader::date_time::parse(
            src.as_bytes(),
        ));
    } else {
        UpdateExpirationDateTime::No
    }
}
