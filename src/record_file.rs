extern crate byteorder;

use self::byteorder::{LE, ReadBytesExt, WriteBytesExt};

use std::cell::RefCell;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom, ErrorKind, Error as IOError};


/// This struct represents the on-disk format of the RecordFile
/// |---------------------------|
/// | H E A D E R ...           |
/// |---------------------------|
/// | num records, 4-bytes      |
/// |---------------------------|
/// | end of file, 8-bytes      |
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
    pub fd: File,          // the actual file
    pub record_count: u32,  // the number of records in the file
    pub header_len: usize, // the length of the header
    pub end_of_file: u64   // the end of the file (size) as controlled by RecordFile
}

impl RecordFile {
    pub fn new(file_path: &str, header: &[u8]) -> Result<RecordFile, Box<Error>> {
        debug!("Attempting to open file: {}", file_path);

        let mut fd = OpenOptions::new().read(true).write(true).create(true).open(&file_path)?;
        let mut record_count = 0;
        let mut end_of_file = (header.len() + 4 + 8) as u64;

        fd.seek(SeekFrom::Start(0))?;

        // check to see if we're opening a new/blank file or not
        if fd.metadata()?.len() == 0 {
            fd.write(header)?;
            fd.write_u32::<LE>(BAD_COUNT)?; // record count
            fd.write_u64::<LE>(end_of_file)?;
            debug!("Created new RecordFile: {}", file_path);
        } else {
            let mut header_buff = vec![0; header.len()];

            fd.read_exact(&mut header_buff)?;

            if header != header_buff.as_slice() {
                return Err(Box::new(IOError::new(ErrorKind::InvalidData, format!("Invalid file header for: {}", file_path))));
            }

            record_count = fd.read_u32::<LE>()?;

            if record_count == BAD_COUNT {
                //TODO: Add a check in here
                panic!("Opened a bad record file");
            }

            end_of_file = fd.read_u64::<LE>()?;

            fd.seek(SeekFrom::Start(end_of_file))?; // go to the end of the file

            debug!("Opened RecordFile: {}", file_path);
        }

        Ok(RecordFile { fd, record_count, header_len: header.len(), end_of_file })
    }

    /// Appends a record to the end of the file
    /// Returns the location where the record was written
    pub fn append(&mut self, record: &[u8]) -> Result<u64, Box<Error>> {
        let rec_loc = self.fd.seek(SeekFrom::Start(self.end_of_file))?;
        let rec_size = record.len();

        self.fd.write_u32::<LE>(rec_size as u32)?;
        self.fd.write(record)?;
        self.fd.flush()?;

        debug!("Wrote record of size: {}", rec_size);

        self.record_count += 1;
        self.end_of_file += (4 + rec_size) as u64;

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
        self.fd.write_u32::<LE>(self.record_count).unwrap(); // cannot return an error, so best attempt
        self.fd.flush().unwrap();

        debug!("Closed RecordFile with {} messages", self.record_count);
    }
}

pub struct RecordFileIterator {
    record_file: RefCell<RecordFile>,
    cur_record: u32
}

impl IntoIterator for RecordFile {
    type Item = Vec<u8>;
    type IntoIter = RecordFileIterator;

    fn into_iter(self) -> Self::IntoIter {
        debug!("Created RecordFileIterator");

        RecordFileIterator{ record_file: RefCell::new(self), cur_record: 0 }
    }
}

impl Iterator for RecordFileIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        // move to the start of the records if this is the first time through
        if self.cur_record == 0 {
            let offset = self.record_file.borrow().header_len as u64 + 4 + 8;
            self.record_file.get_mut().fd.seek(SeekFrom::Start(offset)).unwrap();
        }

        // invariant when we've reached the end of the records
        if self.cur_record >= self.record_file.borrow().record_count {
            return None;
        }

        let rec_size = match self.record_file.get_mut().fd.read_u32::<LE>() {
            Err(e) => { error!("Error reading record file: {}", e.to_string()); return None; },
            Ok(s) => s
        };

        let mut msg_buff = vec![0; rec_size as usize];

        debug!("Reading record of size {}", rec_size);

        if let Err(e) = self.record_file.get_mut().fd.read_exact(&mut msg_buff) {
            error!("Error reading record file: {}", e.to_string());
            return None;
        }

        self.cur_record += 1; // up the count of records read

        Some(msg_buff)
    }
}

pub struct MutRecordFileIterator<'a> {
    record_file: RefCell<&'a mut RecordFile>,
    cur_record: u32
}

impl <'a> IntoIterator for &'a mut RecordFile {
    type Item = Vec<u8>;
    type IntoIter = MutRecordFileIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        debug!("Created RecordFileIterator");

        MutRecordFileIterator{ record_file: RefCell::new(self), cur_record: 0 }
    }
}

impl <'a> Iterator for MutRecordFileIterator<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        // move to the start of the records if this is the first time through
        if self.cur_record == 0 {
            let offset = self.record_file.borrow().header_len as u64 + 4 + 8;
            self.record_file.get_mut().fd.seek(SeekFrom::Start(offset)).unwrap();
        }

        // invariant when we've reached the end of the records
        if self.cur_record >= self.record_file.borrow().record_count {
            return None;
        }

        let rec_size = match self.record_file.get_mut().fd.read_u32::<LE>() {
            Err(e) => { error!("Error reading record file: {}", e.to_string()); return None; },
            Ok(s) => s
        };

        let mut msg_buff = vec![0; rec_size as usize];

        debug!("Reading record of size {}", rec_size);

        if let Err(e) = self.record_file.get_mut().fd.read_exact(&mut msg_buff) {
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
    use std::io::{Read, Write, Seek, SeekFrom, ErrorKind, Error as IOError};

    #[test]
    fn new() {
        simple_logger::init().unwrap();  // this will panic on error
        remove_file("/tmp/test.data");
        let mut rec_file = RecordFile::new("/tmp/test.data", "ABCD".as_bytes()).unwrap();

        rec_file.fd.seek(SeekFrom::Start(rec_file.end_of_file));
        rec_file.fd.write("TEST".as_bytes());
    }

    #[test]
    fn append() {
        simple_logger::init().unwrap();  // this will panic on error
        remove_file("/tmp/test.data");
        let mut rec_file = RecordFile::new("/tmp/test.data", "ABCD".as_bytes()).unwrap();

        // put this here to see if it messes with stuff
        rec_file.fd.seek(SeekFrom::Start(rec_file.end_of_file));
        rec_file.fd.write("TEST".as_bytes());

        let rec = "THE_RECORD".as_bytes();

        let loc = rec_file.append(rec).unwrap();
        assert_eq!(loc, rec_file.end_of_file - (4 + rec.len()) as u64);

        let loc2 = rec_file.append(rec).unwrap();
        assert_eq!(loc2, rec_file.end_of_file - (4 + rec.len()) as u64);
    }

    #[test]
    fn read_at() {
        simple_logger::init().unwrap();  // this will panic on error
        remove_file("/tmp/test.data");
        let mut rec_file = RecordFile::new("/tmp/test.data", "ABCD".as_bytes()).unwrap();
        let rec = "THE_RECORD".as_bytes();

        rec_file.append(rec).unwrap();
        let loc = rec_file.append(rec).unwrap();

        let rec_read = rec_file.read_at(loc).unwrap();

        assert_eq!(rec, rec_read.as_slice());
    }

    #[test]
    fn iterate() {
        simple_logger::init().unwrap();  // this will panic on error
        remove_file("/tmp/test.data");
        let mut rec_file = RecordFile::new("/tmp/test.data", "ABCD".as_bytes()).unwrap();

        // put this here to see if it messes with stuff
        rec_file.fd.seek(SeekFrom::Start(rec_file.end_of_file));
        rec_file.fd.write("TEST".as_bytes());

        let rec = "THE_RECORD".as_bytes();

        let loc = rec_file.append(rec).unwrap();
        assert_eq!(loc, rec_file.end_of_file - (4 + rec.len()) as u64);

        let loc2 = rec_file.append(rec).unwrap();
        assert_eq!(loc2, rec_file.end_of_file - (4 + rec.len()) as u64);

        for rec in rec_file.into_iter() {
            assert_eq!("THE_RECORD".as_bytes(), rec.as_slice());
        }
    }
}