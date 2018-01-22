extern crate serde_json;
extern crate twox_hash;
extern crate base64;
extern crate time;
extern crate byteorder;

use serde_json::{Value, Map};
use serde_json::Error as JsonError;
use serde_json::error::ErrorCode;
use twox_hash::XxHash;
use base64::{Config, CharacterSet, LineWrap};
use byteorder::{LE, WriteBytesExt};

use std::error::Error;
use std::collections::HashMap;
use std::hash::Hasher;

use ::log_value::LogValue;

// this is kinda clunky :-\
fn make_json_error(msg: &str) -> JsonError {
    return JsonError::syntax(ErrorCode::Message(String::from(msg).into_boxed_str()), 0, 0);
}

fn get_ts() -> (i64) {
    let ts = time::get_time();

    return (ts.sec * 1000) + (ts.nsec as i64 / 1000000);
}

pub fn json2map(log: &str) -> Result<HashMap<String, LogValue>, Box<Error>> {
    debug!("Attempting to parse JSON: {}", log);

    let v: Value = serde_json::from_str(log)?;

    // ensure it's an object
    if !v.is_object() {
        return Err(Box::new(make_json_error("Messages must be JSON objects")));
    }

    let mut json_map: Map<String, Value> = v.as_object().unwrap().to_owned();
    let mut log_map = HashMap::<String, LogValue>::new();

    // construct our hash function
    let mut hash: XxHash = XxHash::with_seed(0xBEDBEEF);

    // get a sorted vector of keys to form a canonical representation
    let mut sorted_keys = json_map.keys().cloned().collect::<Vec<_>>();

    // check to see if there are any restricted fields
    if sorted_keys.iter().any(|k| k.starts_with("__")) {
        return Err(From::from("Illegal fields in message; fields cannot start with __: ".to_owned() + log));
    }

    // add the TS to the message
    json_map.insert(String::from("__ts"), json!(get_ts()));

    // sort the keys so we get a canonical order
    sorted_keys.sort_unstable();

    // go through each sorted key, and convert it into a LogValue
    for key in sorted_keys.into_iter() {
        let value = json_map.get(&key).unwrap(); // should be safe

        // we don't support nested objects
        if let &Value::Object(_) = value {
            return Err(Box::new(make_json_error("Nested JSON Objects are not allowed")));
        }

        // convert to a LogValue or return an error if nested objects found
        let log_value = if let Value::Array(ref v) = *value {
            if v.iter().any(|i| i.is_object()) {
                return Err(Box::new(make_json_error("JSON Objects in arrays not allowed")));
            }

            let mut log_value_array = v.iter().map(|x| LogValue::from(x)).collect::<Vec<_>>();

            log_value_array.sort_unstable(); // sort the array

            LogValue::Array(log_value_array)
        } else {
            LogValue::from(value)
        };

        // hash both
        hash.write(key.as_bytes());
        hash.write(&log_value.as_bytes());

        // add to the log_map
        log_map.insert(key.to_owned(), log_value);
    }

    let mut buff = vec![];

    buff.write_u64::<LE>(hash.finish())?;

    let id = base64::encode_config(&buff, Config::new(CharacterSet::Standard, false, true, LineWrap::NoWrap));

    debug!("ID: {}", id);

    // add the ID to the message
    log_map.insert(String::from("__id"), LogValue::String(String::from(id)));

    Ok(log_map)
}