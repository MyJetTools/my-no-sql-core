#[derive(Debug, Clone)]
pub struct JsonKeyValuePosition {
    pub key_start: usize,
    pub key_end: usize,
    pub value_start: usize,
    pub value_end: usize,
}
