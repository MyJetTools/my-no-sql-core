mod db_table;
#[cfg(feature = "master-node")]
mod db_table_attributes;
#[cfg(feature = "master-node")]
pub use db_table_attributes::DbTableAttributes;

pub use db_table::DbTable;
