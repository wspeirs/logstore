extern crate byteorder;

use self::byteorder::{LE, ReadBytesExt, WriteBytesExt};

use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom, ErrorKind, Error as IOError};


/// This struct represents the on-disk format of the RecordFile
/// |---------------------------|
/// | H E A D E R ...           |
/// |---------------------------|
/// | num records, 4-bytes      |
/// |---------------------------|
/// | record size, 4-bytes      |
/// |---------------------------|
/// | record ...                |
/// |---------------------------|
/// | ...                       |
/// |---------------------------|

pub const BAD_COUNT: u32 = 0xFFFFFFFF;

/// Record file
pub struct RecordFile {
    pub fd: File, // the actual file
    pub num_records: u32, // the number of records in the file
    pub header_len: usize // the length of the header
}

impl RecordFile {
    pub fn new(file_path: &str, header: &[u8]) -> Result<RecordFile, Box<Error>> {
        debug!("Attempting to open file: {}", file_path);

        let mut fd = OpenOptions::new().read(true).write(true).create(true).open(&file_path)?;
        let mut num_records = 0;

        fd.seek(SeekFrom::Start(0))?;

        // check to see if we're opening a new/blank file or not
        if fd.metadata()?.len() == 0 {
            fd.write(header)?;
            fd.write_u32::<LE>(BAD_COUNT)?;
            debug!("Created new RecordFile: {}", file_path);
        } else {
            let mut header_buff = vec![0; header.len()];

            fd.read_exact(&mut header_buff)?;

            if header != header_buff.as_slice() {
                return Err(Box::new(IOError::new(ErrorKind::InvalidData, format!("Invalid file header for: {}", file_path))));
            }

            num_records = fd.read_u32::<LE>()?;
            fd.seek(SeekFrom::End(0))?; // go to the end of the file

            debug!("Opened RecordFile: {}", file_path);
        }

        Ok(RecordFile { fd, num_records, header_len: header.len() })
    }

    /// Appends a record to the end of the file
    /// Returns the location where the record was written
    pub fn append(&mut self, record: &[u8]) -> Result<u64, Box<Error>> {
        let rec_loc = self.fd.seek(SeekFrom::Current(0))?;
        let rec_size = record.len();

        self.fd.write_u32::<LE>(rec_size as u32)?;
        self.fd.write(record)?;
        self.fd.flush()?;

        debug!("Wrote record of size: {}", rec_size);

        self.num_records += 1;

        Ok(rec_loc)
    }

    /// Read a record from a given offset
    pub fn read_at(&mut self, file_offset: u64) -> Result<Vec<u8>, Box<Error>> {
        self.fd.seek(SeekFrom::Start(file_offset))?;

        let rec_size = self.fd.read_u32::<LE>()?;
        let mut rec_buff = vec![0; rec_size as usize];

        self.fd.read_exact(&mut rec_buff)?;

        Ok(rec_buff)
    }
}

impl Drop for RecordFile {
    fn drop(&mut self) {
        self.fd.seek(SeekFrom::Start(self.header_len as u64)).unwrap();
        self.fd.write_u32::<LE>(self.num_records).unwrap(); // cannot return an error, so best attempt
        self.fd.flush().unwrap();

        debug!("Closed RecordFile with {} messages", self.num_records);
    }
}

pub struct RecordFileIterator<'a> {
    record_file: &'a mut RecordFile,
    cur_record: u32
}

impl <'a> IntoIterator for &'a mut RecordFile {
    type Item = Vec<u8>;
    type IntoIter = RecordFileIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        // move to the beginning of the messages
        self.fd.seek(SeekFrom::Start(self.header_len as u64 + 4)).unwrap();

        debug!("Created RecordFileIterator");

        RecordFileIterator{ record_file: self, cur_record: 0 }
    }
}

impl <'a> Iterator for RecordFileIterator<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        // invariant when we've reached the end of the records
        if self.cur_record >= self.record_file.num_records {
            return None;
        }

        let rec_size = match self.record_file.fd.read_u32::<LE>() {
            Err(e) => { error!("Error reading record file: {}", e.to_string()); return None; },
            Ok(s) => s
        };

        let mut msg_buff = vec![0; rec_size as usize];

        debug!("Reading record of size {}", rec_size);

        if let Err(e) = self.record_file.fd.read_exact(&mut msg_buff) {
            error!("Error reading record file: {}", e.to_string());
            return None;
        }

        self.cur_record += 1; // up the count of records read

        Some(msg_buff)
    }
}

#[cfg(test)]
mod tests {
    use ::record_file::RecordFile;
    use simple_logger;
    use std::fs::remove_file;

    #[test]
    fn new() {
        simple_logger::init().unwrap();  // this will panic on error
        remove_file("/tmp/test.data").unwrap();
        RecordFile::new("/tmp/test.data", "ABCD".as_bytes()).unwrap();
    }

    #[test]
    fn append() {
        simple_logger::init().unwrap();  // this will panic on error
        remove_file("/tmp/test.data").unwrap();
        let mut rec_file = RecordFile::new("/tmp/test.data", "ABCD".as_bytes()).unwrap();
        let rec = "THE_RECORD".as_bytes();

        let loc = rec_file.append(rec).unwrap();

        assert_eq!(loc, 8);

        let loc2 = rec_file.append(rec).unwrap();

        assert_eq!(loc2, loc+4+rec.len() as u64);
    }

    #[test]
    fn read_at() {
        simple_logger::init().unwrap();  // this will panic on error
        remove_file("/tmp/test.data").unwrap();
        let mut rec_file = RecordFile::new("/tmp/test.data", "ABCD".as_bytes()).unwrap();
        let rec = "THE_RECORD".as_bytes();

        rec_file.append(rec).unwrap();
        let loc = rec_file.append(rec).unwrap();

        let rec_read = rec_file.read_at(loc).unwrap();

        assert_eq!(rec, rec_read.as_slice());
    }
}