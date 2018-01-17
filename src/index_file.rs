extern crate byteorder;
extern crate multimap;

use self::byteorder::{LE, ReadBytesExt, WriteBytesExt};
use self::multimap::MultiMap;

use std::error::Error;

use ::log_value::LogValue;
use ::record_file::{RecordFile, RecordFileIterator, BAD_COUNT};
use std::io::{Read, Write, Seek, SeekFrom, ErrorKind, Error as IOError};

const FILE_HEADER: &[u8; 12] = b"LOGINDEX\x01\x00\x00\x00";

/// This is the on-disk structure of the index file
/// |---------------------------------------|
/// | Record file ...                       |
/// |---------------------------------------|
/// | term_start (u64)  term_len (u16)      |
/// |---------------------------------------|
/// | term_start (u64)  term_len (u16)      |
/// |---------------------------------------|
/// | term_start (u64)  term_len (u16)      |
/// |---------------------------------------|

pub struct IndexFile {
    rec_file: RecordFile, // the record file
    index: MultiMap<LogValue, u64> // the in-memory index
}

impl IndexFile  {
    pub fn new(dir_path: &str, index_name: &str) -> Result<IndexFile, Box<Error>> {
        let mut file_path = String::from(dir_path);

        if !file_path.ends_with("/") {
            file_path.push_str("/")
        }

        file_path.push_str(index_name);
        file_path.push_str(".index");

        let rec_file = RecordFile::new(&file_path, FILE_HEADER)?;

        // TODO: Run a check on this file

        Ok(IndexFile { rec_file, index: MultiMap::new() })
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

impl Drop for IndexFile {
    fn drop(&mut self) {
//        self.fd.seek(SeekFrom::Start(self.header_len as u64)).unwrap();
//        self.fd.write_u32::<LE>(self.num_records).unwrap(); // cannot return an error, so best attempt
//        self.fd.flush().unwrap();
//
//        debug!("Closed RecordFile with {} messages", self.num_records);
    }
}


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
