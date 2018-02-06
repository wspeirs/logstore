use rmps::encode::to_vec;
use rmps::decode::from_slice;

use std::collections::HashMap;
use std::path::Path;
use std::vec::IntoIter;

use ::record_file::{RecordFile, RecordFileIterator, BAD_COUNT};
use ::log_value::LogValue;
use ::record_error::RecordError;

const FILE_HEADER: &[u8; 12] = b"LOGSTORE\x01\x00\x00\x00";

/// The log file that holds all of the log messages
pub struct LogFile {
    rec_file: RecordFile,
}

impl LogFile {
    /// Creates a new LogFile
    pub fn new(dir_path: &Path) -> Result<LogFile, RecordError> {
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
    pub fn check(&mut self) -> Result<u32, RecordError> {
        let mut count = 0;

        for rec in (&mut self.rec_file).into_iter() {
            from_slice::<HashMap<String, LogValue>>(&rec)?;
            count += 1;
        }

        Ok(count)
    }

    /// Adds a log to the file, returning the location in the file
    pub fn add(&mut self, log: &HashMap<String, LogValue>) -> Result<u64, RecordError> {
        let buff = to_vec(log)?;

        // write the record file
        let loc = self.rec_file.append(&buff)?;

        return Ok(loc);
    }

    pub fn get(&self, location: u64) -> Result<HashMap<String, LogValue>, RecordError> {
        match from_slice::<HashMap<String, LogValue>>(self.rec_file.read_at(location)?.as_slice()) {
            Err(e) => Err(RecordError::from(e)),
            Ok(v) => Ok(v)
        }
    }

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
    use ::json::json2map;

    use std::path::Path;
    use simple_logger;


    #[test]
    fn new_file_no_slash() {
        simple_logger::init().unwrap();  // this will panic on error
        LogFile::new(Path::new("/tmp")).unwrap();
    }

    #[test]
    fn new_file_with_slash() {
        simple_logger::init().unwrap();  // this will panic on error
        LogFile::new(Path::new("/tmp/")).unwrap();
    }

    #[test]
    fn check_file() {
        simple_logger::init().unwrap();  // this will panic on error
        let mut log_file = LogFile::new(Path::new("/tmp/")).unwrap();
        let num_logs = log_file.rec_file.record_count;

        assert_eq!(num_logs, log_file.check().unwrap());
    }

    #[test]
    fn add_valid_msg() {
        simple_logger::init().unwrap();  // this will panic on error
        let mut msg_file = LogFile::new(Path::new("/tmp")).unwrap();
        let msg = json!({
            "d": 23,
            "c": null,
            "b": true,
            "a": "something"
        });

        let id = msg_file.add(&json2map(&msg.to_string()).unwrap()).unwrap();

        println!("ID: {}", id);
    }

    #[test]
    fn add_nested_json() {
        simple_logger::init().unwrap();  // this will panic on error
        let mut msg_file = LogFile::new(Path::new("/tmp")).unwrap();
        let msg = json!({
            "c": "test",
            "b": 23,
            "a": { "x": "z" }
        });

        // this should be an error
        assert!(msg_file.add(&json2map(&msg.to_string()).unwrap()).is_err());
    }

    #[test]
    fn add_illegal_field() {
        simple_logger::init().unwrap();  // this will panic on error
        let mut msg_file = LogFile::new(Path::new("/tmp")).unwrap();
        let msg = json!({
            "__c": "test",
            "b": 23,
            "a": true
        });

        // this should be an error
        assert!(msg_file.add(&json2map(&msg.to_string()).unwrap()).is_err());
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