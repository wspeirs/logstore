extern crate serde_json;
extern crate twox_hash;
extern crate base32;
extern crate time;
extern crate byteorder;

use serde_json::{Value, Map};
use serde_json::Error as JsonError;
use serde_json::error::ErrorCode;
use self::twox_hash::XxHash;
use self::base32::Alphabet; // TODO: Switch this to base64
use self::byteorder::{LE, WriteBytesExt};

use std::str;
use std::hash::Hasher;
use std::error::Error;
use std::io::{ErrorKind, Error as IOError};

use ::record_file::{RecordFile, RecordFileIterator, BAD_COUNT};

const FILE_HEADER: &[u8; 12] = b"LOGSTORE\x01\x00\x00\x00";

// this is kinda clunky :-\
fn make_json_error(msg: &str) -> JsonError {
    return JsonError::syntax(ErrorCode::Message(String::from(msg).into_boxed_str()), 0, 0);
}

fn get_ts() -> (i64) {
    let ts = time::get_time();

    return (ts.sec * 1000) + (ts.nsec as i64 / 1000000);
}

/// The log file that holds all of the log messages
pub struct LogFile {
    rec_file: RecordFile
}

impl LogFile {
    /// Creates a new LogFile
    pub fn new(dir_path: &str) -> Result<LogFile, Box<Error>> {
        let mut file_path = String::from(dir_path);

        if file_path.ends_with("/") {
            file_path.push_str("logs.data");
        } else {
            file_path.push_str("/logs.data");
        }

        let rec_file = RecordFile::new(&file_path, FILE_HEADER)?;
        let mut ret = LogFile{ rec_file };

        if ret.rec_file.num_records == BAD_COUNT {
            error!("{} not properly closed, attempting to check file", file_path);

            return match ret.check() {
                Ok(count) => { info!("Read {} messages from file successfully", count); Ok(ret) },
                Err(e) => Err(From::from(e))
            };
        } else {
            return Ok(ret);
        }
   }

    ///
    /// Checks the file attempting to read each JSON message, and re-establish the count
    ///
    pub fn check(&mut self) -> Result<u32, Box<Error>> {
        let mut count = 0;

        for rec in self.rec_file.into_iter() {
            match serde_json::from_slice(rec.as_slice()) {
                Err(e) => {
                    error!("Error checking file: {}", e.to_string());
                    return Err(Box::new(e));
                }, Ok(v) => {
                    if let Value::Object(_) = v {
                        count += 1;
                    } else {
                        error!("Found non-JSON object while checking file");
                        return Err(Box::new(
                            IOError::new(ErrorKind::InvalidData, "Found non-JSON object while checking file")
                        ));
                    }
                }
            };
        }

        // update the cound in the underlying RecordFile
        self.rec_file.num_records = count;

        Ok(count)
    }

    /// Adds a log to the file
    pub fn add(&mut self, message: &str) -> Result<String, Box<Error>> {
        trace!("Attempting to parse JSON: {}", message);

        let v: Value = serde_json::from_str(message)?;

        trace!("Parsed JSON");

        // ensure it's an object
        if !v.is_object() {
            return Err(Box::new(make_json_error("Messages must be JSON objects")));
        }

        let mut json: Map<String, Value> = v.as_object().unwrap().to_owned();

        trace!("Checking all fields for __");

        let mut canoncial_json = String::from("{");

        { // this wraps all the immutable borrows
            // check to see if there are any restricted fields
            if json.keys().any(|k| k.starts_with("__")) {
                return Err(From::from("Illegal fields in message; fields cannot start with __: ".to_owned() + message));
            }

            // add the TS to the message
            json.insert(String::from("__ts"), json!(get_ts()));

            // get a sorted vector of keys to form a canonical representation
            let mut sorted_keys = json.keys().collect::<Vec<&String>>();

            sorted_keys.sort_unstable();

            // go through each sorted key, and convert it into a string based upon type
            // or return an error if a nested JSON object is found
            for key in sorted_keys.into_iter() {
                let value = match json.get(key).unwrap().to_owned() {
                    Value::Object(_) => Err(make_json_error("Nested JSON Objects are not allowed")),
                    Value::Null => Ok(String::from("null")),
                    Value::Bool(b) => Ok(b.to_string()),
                    Value::Number(n) => Ok(n.to_string()),
                    Value::String(s) => Ok(format!("'{}'", s)),
                    //TODO: Need to canoncialize this too
                    Value::Array(v) => Ok(v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(","))
                }?;

                // add key: value
                canoncial_json.push_str(&format!("{}:{},", key.to_owned(), value));
            }
        }

        canoncial_json.pop(); // remove trailing comma
        canoncial_json.push_str("}");

        debug!("Canonical: {}", canoncial_json);

        // construct our hash function
        let mut hash: XxHash = XxHash::with_seed(0xBEDBEEF);

        hash.write(canoncial_json.as_bytes());

        let mut buff = vec![];

        buff.write_u64::<LE>(hash.finish())?;

        let id = base32::encode(Alphabet::RFC4648 { padding: false }, &buff);

        debug!("ID: {}", id);

        // add the ID to the message
        json.insert(String::from("__id"), Value::String(id.to_owned()));

        let final_message = serde_json::to_string(&json)?;

        debug!("Final Message: {} {}", final_message.len(), final_message);

        // write the JSON to the record file
        self.rec_file.append(final_message.as_bytes())?;

        return Ok(id.to_owned());
    }

//    pub fn tombstone(&mut self, id: &str) -> Result<bool, Box<Error>> {
//        let mut prev_pos = NUM_MSG_OFFSET + 4; // start of the messages
//        let mut found = false;
//
//        for mut block in self.into_iter() {
//            let mut cur = Cursor::new(block);
//            cur.seek(SeekFrom::Start(1))?; // move past the tombstone
//
//            let msg_size = cur.read_u32::<LE>().unwrap() as u64;
//            block = cur.into_inner();
//
//            let message = str::from_utf8(&block[5..])?;
//
//            let json = match serde_json::from_str(&message) {
//                Err(e) => {
//                    warn!("Error parsing message into JSON: {}", e.to_string());
//                    debug!("MSG: {}", message);
//                    return Err(From::from(e));
//                },
//                Ok(x) => match x {
//                    Value::Object(o) => o,
//                    _ => {
//                        warn!("Found non-object JSON: {}", message);
//                        return Err(From::from("Found non-object JSON"));
//                    }
//                }
//            };
//
//            if json.get("__id").unwrap() == id {
//                debug!("Found id to tombstone");
//                found = true;
//                break;
//            }
//
//            prev_pos += 5 + msg_size;
//        }
//
//        if found {
//            // go back to the start of the other message
//            self.fd.seek(SeekFrom::Start(prev_pos))?;
//
//            debug!("Tombstoning at {}", prev_pos);
//
//            // write out that we want to tombstone it
//            self.fd.write_u8(0x01)?;
//
//            // go to the end of the file
//            self.fd.seek(SeekFrom::End(0))?;
//
//            return Ok(true);
//        } else {
//            return Ok(false); // we weren't able to find it
//        }
//    }
}

pub struct LogFileIterator<'a> {
    rec_iter: RecordFileIterator<'a>
}

impl <'a> IntoIterator for &'a mut LogFile {
    type Item = Map<String, Value>;
    type IntoIter = LogFileIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        LogFileIterator{ rec_iter: self.rec_file.into_iter() }
    }
}

impl <'a> Iterator for LogFileIterator<'a> {
    type Item = Map<String, Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let rec = self.rec_iter.next();

        if rec.is_none() {
            return None;
        }

        match serde_json::from_slice(rec.unwrap().as_slice()) {
            Err(e) => {
                error!("Error parsing JSON: {}", e.to_string());
                return None;
            }, Ok(v) => {
                if let Value::Object(m) = v {
                    return Some(m);
                } else {
                    error!("Found non-JSON object in record");
                    return None;
                }
            }
        }

    }
}

#[cfg(test)]
mod tests {
    use ::log_file::LogFile;
    use simple_logger;


    #[test]
    fn new_file_no_slash() {
        simple_logger::init().unwrap();  // this will panic on error
        LogFile::new("/tmp").unwrap();
    }

    #[test]
    fn new_file_with_slash() {
        simple_logger::init().unwrap();  // this will panic on error
        LogFile::new("/tmp/").unwrap();
    }

    #[test]
    fn check_file() {
        simple_logger::init().unwrap();  // this will panic on error
        let mut log_file = LogFile::new("/tmp/").unwrap();
        let num_logs = log_file.rec_file.num_records;

        assert_eq!(num_logs, log_file.check().unwrap());
    }

    #[test]
    fn add_valid_msg() {
        simple_logger::init().unwrap();  // this will panic on error
        let mut msg_file = LogFile::new("/tmp").unwrap();
        let msg = r#"{
            "d": 23,
            "c": null,
            "b": true,
            "a": "something"
        }"#;

        let id = msg_file.add(msg).unwrap();

        println!("ID: {}", id);
    }

    #[test]
    fn add_nested_json() {
        simple_logger::init().unwrap();  // this will panic on error
        let mut msg_file = LogFile::new("/tmp").unwrap();
        let msg = r#"{
            "c": "test",
            "b": 23,
            "a": { "x": "z" }
        }"#;

        // this should be an error
        assert!(msg_file.add(msg).is_err());
    }

    #[test]
    fn add_illegal_field() {
        simple_logger::init().unwrap();  // this will panic on error
        let mut msg_file = LogFile::new("/tmp").unwrap();
        let msg = r#"{
            "__c": "test",
            "b": 23,
            "a": true
        }"#;

        // this should be an error
        assert!(msg_file.add(msg).is_err());
    }

//    #[test]
//    fn tombstone_message() {
//        simple_logger::init().unwrap();  // this will panic on error
//        let mut msg_file = LogFile::new("/tmp").unwrap();
//        let msg = r#"{
//            "z": "test"
//        }"#;
//        let id = msg_file.add(msg).unwrap();
//
//        assert!(msg_file.tombstone(id.as_str()).unwrap());
//    }
}