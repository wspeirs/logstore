extern crate rmp_serde as rmps;
extern crate twox_hash;
extern crate base32;
extern crate time;
extern crate byteorder;

use rmps::encode::to_vec;
use rmps::decode::{from_slice, from_read};
use self::twox_hash::XxHash;
use self::base32::Alphabet; // TODO: Switch this to base64
use self::byteorder::{LE, WriteBytesExt};

use std::str;
use std::hash::Hasher;
use std::error::Error;
use std::io::{ErrorKind, Error as IOError};
use std::collections::HashMap;
use std::path::Path;

use ::record_file::{RecordFile, RecordFileIterator, BAD_COUNT};
use ::log_value::LogValue;

const FILE_HEADER: &[u8; 12] = b"LOGSTORE\x01\x00\x00\x00";

/// The log file that holds all of the log messages
pub struct LogFile {
    rec_file: RecordFile
}

impl LogFile {
    /// Creates a new LogFile
    pub fn new(dir_path: &Path) -> Result<LogFile, Box<Error>> {
        let file_path = dir_path.join("logs.data");

        let rec_file = RecordFile::new(&file_path, FILE_HEADER)?;
        let mut ret = LogFile{ rec_file };

        if ret.rec_file.record_count == BAD_COUNT {
            error!("{} not properly closed, attempting to check file", file_path.display());

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

        for rec in (&mut self.rec_file).into_iter() {
            from_slice::<HashMap<String, LogValue>>(&rec)?;
            count += 1;
        }

        Ok(count)
    }

    /// Adds a log to the file, returning the location in the file
    pub fn add(&mut self, log: &HashMap<String, LogValue>) -> Result<u64, Box<Error>> {
        let buff = to_vec(log)?;

        // write the record file
        let loc = self.rec_file.append(&buff)?;

        return Ok(loc);
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

pub struct LogFileIterator {
    rec_iter: RecordFileIterator
}

impl IntoIterator for LogFile {
    type Item = HashMap<String, LogValue>;
    type IntoIter = LogFileIterator;

    fn into_iter(self) -> Self::IntoIter {
        LogFileIterator{ rec_iter: self.rec_file.into_iter() }
    }
}

impl Iterator for LogFileIterator {
    type Item = HashMap<String, LogValue>;

    fn next(&mut self) -> Option<Self::Item> {
        let rec = self.rec_iter.next();

        if rec.is_none() {
            return None;
        }


        match from_slice(&rec.unwrap()) {
            Err(e) => {
                error!("Error parsing Log: {}", e.to_string());
                return None;
            }, Ok(v) => return Some(v)
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
        let num_logs = log_file.rec_file.record_count;

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