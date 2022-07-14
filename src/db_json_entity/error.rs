use my_json::json_reader::JsonParseError;

#[derive(Debug)]
pub enum DbEntityParseFail {
    FieldPartitionKeyIsRequired,
    FieldRowKeyIsRequired,
    FieldPartitionKeyCanNotBeNull,
    FieldRowKeyCanNotBeNull,
    JsonParseError(JsonParseError),
    PartitionKeyIsTooLong,
}

impl From<JsonParseError> for DbEntityParseFail {
    fn from(src: JsonParseError) -> Self {
        Self::JsonParseError(src)
    }
}
