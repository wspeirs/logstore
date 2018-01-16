//extern crate serde_json;
extern crate byteorder;
extern crate multimap;

use self::byteorder::{LE, ReadBytesExt, WriteBytesExt};
use self::multimap::MultiMap;

use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use ::log_value::LogValue;

pub struct IndexFile {
    fd: File, // the actual message file
    num_records: u32, // the number of records in the file
    index: MultiMap<LogValue, u64> // the in-memory index
}

/// This struct represents the on-disk format of the IndexFile
/// VV is the version number and 0xTT = if the record has been tombstoned:
/// |-------------------------------------------|
/// | 0x4C 0x4F 0x47 0x53 | 0x54 0x4F 0x52 0x58 |
/// | L    O    G    I    | N    D    E    X    |
/// |-------------------------------------------|
/// | 0xVV 0x00 0x00 0x00 | num records in file |
/// |-------------------------------------------|
/// | record size         | 0xTT| record...     |
/// |-------------------------------------------|
/// | ...                                       |
/// |-------------------------------------------|

const FILE_HEADER: &[u8; 12] = b"LOGINDEX\x01\x00\x00\x00";
const BAD_COUNT: u32 = 0xFFFFFFFF; // TODO: Share w/MessageFile
const RECORD_START: u64 = 16;

impl IndexFile  {
    pub fn new(dir_path: &str, index_name: &str) -> Result<IndexFile, Box<Error>> {
        let mut file_path = String::from(dir_path);

        if !file_path.ends_with("/") {
            file_path.push_str("/")
        }

        file_path.push_str(index_name);
        file_path.push_str(".index");

        debug!("Attempting to open index file: {}", file_path);

        let mut index_file = OpenOptions::new().read(true).write(true).create(true).open(&file_path)?;
        let mut num_records = 0;

        index_file.seek(SeekFrom::Start(0))?;

        // check to see if we're opening a new/blank file or not
        if index_file.metadata()?.len() == 0 {
            index_file.write(FILE_HEADER)?;
            index_file.write_u32::<LE>(BAD_COUNT)?;
            info!("Created new IndexFile {}: {}", index_name, file_path);
        } else {
            let mut header = vec![0; 12];

            index_file.read_exact(&mut header)?;

            if FILE_HEADER != header.as_slice() {
                return Err(From::from(format!("Invalid file header for index file: {}", file_path)));
            }

            num_records = index_file.read_u32::<LE>()?;
            index_file.seek(SeekFrom::End(0))?; // go to the end of the file

            info!("Opened IndexFile: {}", file_path);
        }

        // TODO: Run a check on this file

        Ok(IndexFile {
            fd: index_file,
            num_records: num_records,
            index: MultiMap::new() // TODO: Set capacity
        })
    }

    pub fn add(&mut self, value: LogValue, offset: u64) {
        // simply add to the in-memory index
        // it's flushed to disk on close
        self.index.insert(value, offset);
    }

    /// Flushes the in-memory index to disk
    fn flush(&mut self) {

    }
}

//struct IndexFileIterator<'a> {
//    index_file: &'a mut IndexFile
//}
//
//impl <'a> IntoIterator for &'a mut IndexFile {
//    type Item = Vec<u8>;
//    type IntoIter = IndexFileIterator<'a>;
//
//    fn into_iter(self) -> Self::IntoIter {
//        // move to the beginning of the messages
//        self.fd.seek(SeekFrom::Start(RECORD_START)).unwrap();
//
//        // get the size of the file
//        let file_size = self.fd.metadata().unwrap().len();
//
//        debug!("Created IndexFileIterator");
//
//        IndexFileIterator{
//            index_file: self
//        }
//    }
//}



#[cfg(test)]
mod tests {
    use ::index_file::IndexFile;
    use simple_logger;

    #[test]
    fn new_file_no_slash() {
        simple_logger::init().unwrap();  // this will panic on error
        IndexFile::new("/tmp", "id").unwrap();
    }

}
