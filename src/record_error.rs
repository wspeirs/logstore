//
// Represents any error where a record is attempted to be read from disk
// Ironically, RecordFile only ever produces io::Error as it only deals with files
// But there are also serialization errors, and this error type represents both
//

use std::io;
use rmps::{encode, decode};

#[derive(Debug, Clone)]
pub enum RecordError {
    Io(io::Error),
    Encode(encode::Error),
    Decode(decode::Error)
}

impl From<io::Error> for RecordError {
    fn from(err: io::Error) -> RecordError {
        RecordError::Io(err)
    }
}

impl From<encode::Error> for RecordError {
    fn from(err: encode::Error) -> RecordError {
        RecordError::Encode(err)
    }
}

impl From<decode::Error> for RecordError {
    fn from(err: decode::Error) -> RecordError {
        RecordError::Decode(err)
    }
}

