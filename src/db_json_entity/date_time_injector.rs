use super::{JsonKeyValuePosition, JsonTimeStamp};

pub fn replace_timestamp_value(
    raw: &[u8],
    time_stamp_position: &JsonKeyValuePosition,
    json_time_stamp: &JsonTimeStamp,
) -> Vec<u8> {
    let timestamp_value = format!("{dq}{val}{dq}", dq = '"', val = json_time_stamp.as_str());

    let timestamp_value = timestamp_value.as_bytes();

    let new_len = timestamp_value.len()
        + (raw.len() - (time_stamp_position.value_end - time_stamp_position.value_start));

    let mut result = Vec::with_capacity(new_len);

    result.extend_from_slice(&raw[..time_stamp_position.key_start + 1]);
    let time_stamp_as_bytes = super::consts::TIME_STAMP.as_bytes();
    result.extend_from_slice(time_stamp_as_bytes);

    result.extend_from_slice(
        &raw[time_stamp_position.key_start + 1 + time_stamp_as_bytes.len()
            ..time_stamp_position.value_start],
    );

    result.extend_from_slice(timestamp_value);

    let after = &raw[time_stamp_position.value_end..];
    result.extend_from_slice(after);

    return result;
}

pub fn inject(raw: &[u8], time_stamp: &JsonTimeStamp) -> Vec<u8> {
    let date_time = format!(
        ",{dq}{ts}{dq}:{dq}{val}{dq}",
        dq = '"',
        ts = super::consts::TIME_STAMP,
        val = time_stamp.as_str(),
    );

    let date_time = date_time.as_bytes();

    let end_of_json = get_the_end_of_the_json(raw);

    let mut result = Vec::with_capacity(raw.len() + date_time.len());

    result.extend_from_slice(&raw[..end_of_json]);

    result.extend_from_slice(date_time);

    result.extend_from_slice(&raw[end_of_json..]);
    result
}

fn get_the_end_of_the_json(data: &[u8]) -> usize {
    for i in (0..data.len()).rev() {
        if data[i] == my_json::json_reader::consts::CLOSE_BRACKET {
            return i;
        }
    }

    panic!("Invalid Json. Can not find the end of json");
}

#[cfg(test)]
mod tests {

    use crate::db_json_entity::{
        date_time_injector::JsonKeyValuePosition, json_time_stamp::JsonTimeStamp,
    };

    #[test]
    fn test_timestamp_injection() {
        let json_ts = JsonTimeStamp::now();

        let src_json = r#"{"Field1":"Value1"} "#;

        let dest_json_etalon = format!(
            "{}\"Field1\":\"Value1\",\"TimeStamp\":\"{}\"{} ",
            '{',
            json_ts.as_str(),
            '}'
        );

        let result = super::inject(src_json.as_bytes(), &json_ts);

        let dest_json = String::from_utf8(result).unwrap();

        println!("{}", dest_json_etalon);
        println!("{}", dest_json);

        assert_eq!(dest_json_etalon, dest_json);
    }

    #[test]
    fn test_replace_null_to_timestamp_and_change_timestamp_from_lowerkey() {
        let json_ts = JsonTimeStamp::now();

        let src_json = r#"{"Field1":"Value1","timestamp":null}"#;

        let dest_json_result = format!(
            "{}\"Field1\":\"Value1\",\"TimeStamp\":\"{}\"{}",
            '{',
            json_ts.as_str(),
            '}'
        );

        let key_start = src_json.find("\"timestamp").unwrap();

        let value_start = src_json.find("null").unwrap();

        let ts_value_position = JsonKeyValuePosition {
            key_start,
            key_end: key_start + super::super::consts::TIME_STAMP.len() + 2,
            value_start,
            value_end: value_start + 4,
        };

        let result =
            super::replace_timestamp_value(src_json.as_bytes(), &ts_value_position, &json_ts);

        let dest_json = String::from_utf8(result).unwrap();

        println!("{}", dest_json_result);
        println!("{}", dest_json);

        assert_eq!(dest_json_result, dest_json);
    }

    #[test]
    fn test_replace_some_string_to_timestamp() {
        let json_ts = JsonTimeStamp::now();

        let src_json = r#"{"Field1":"Value1","TimeStamp":"ReplaceHere"}"#;

        let dest_json_etalon = format!(
            "{}\"Field1\":\"Value1\",\"TimeStamp\":\"{}\"{}",
            '{',
            json_ts.as_str(),
            '}'
        );

        let key_start = src_json.find("\"TimeStamp").unwrap();

        let replace_string = "\"ReplaceHere\"";

        let value_start = src_json.find(replace_string).unwrap();

        let ts_value_position = JsonKeyValuePosition {
            key_start,
            key_end: key_start + super::super::consts::TIME_STAMP.len() + 2,
            value_start,
            value_end: value_start + replace_string.len(),
        };

        let result =
            super::replace_timestamp_value(src_json.as_bytes(), &ts_value_position, &json_ts);

        let dest_json = String::from_utf8(result).unwrap();

        println!("{}", dest_json_etalon);
        println!("{}", dest_json);

        assert_eq!(dest_json_etalon, dest_json);
    }
}
