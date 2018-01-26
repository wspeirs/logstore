extern crate byteorder;
extern crate multimap;
extern crate rmp_serde as rmps;
extern crate itertools;

use rmps::encode::to_vec;
use rmps::decode::{from_slice, from_read};

//use self::byteorder::{LE, ReadBytesExt, WriteBytesExt};
use self::multimap::MultiMap;

use std::error::Error;
use std::collections::HashMap;
use std::fs::{remove_file, rename};
use std::path::{Path, PathBuf};
use itertools::Itertools;
use itertools::EitherOrBoth::{Left, Right, Both};

use ::log_value::LogValue;
use ::record_file::{RecordFile, buf2string};
use std::io::{Write, Seek, SeekFrom};

//const FILE_HEADER: &[u8; 12] = b"LOGINDEX\x01\x00\x00\x00";
const FILE_HEADER: &[u8; 12] = b"LOGINDEX\x01XXX";

/// This is the on-disk structure of the index file
/// |----------------------------------------|
/// | term: offset in data file (term_entry) |
/// |----------------------------------------|
/// | ....                                   |
/// |----------------------------------------|
/// | term: offset in data file (term_entry) |
/// |----------------------------------------|
/// | serialized term map                    |
/// |----------------------------------------|

pub struct IndexFile {
    rec_file: RecordFile,               // the record file holding the term -> Vec<offsets in file>
    mem_index: MultiMap<LogValue, u64>, // not-yet-persisted index entries
    term_map: HashMap<LogValue, u64>,   // term to location in index file
    dir_path: PathBuf,
    index_name: String
}

impl IndexFile  {
    pub fn new(dir_path: &Path, index_name: &str) -> Result<IndexFile, Box<Error>> {
        let file_path = dir_path.join(index_name.to_owned() + ".index");
        let mut rec_file = RecordFile::new(&file_path, FILE_HEADER)?;

        // the end of the record file, is where the serialized term map begins
        let eof = rec_file.fd.seek(SeekFrom::End(0))?;
        let begin = rec_file.fd.seek(SeekFrom::Start(rec_file.end_of_file))?;
        let mut term_map = HashMap::new();

        if eof-begin > 0 {
            term_map = from_read(&rec_file.fd)?;
            debug!("Read in {} terms from index {}", term_map.len(), index_name);
        }

        // TODO: Run a check on this file

        Ok(IndexFile {
            rec_file,
            mem_index: MultiMap::new(),
            term_map,
            dir_path: PathBuf::from(dir_path),
            index_name: String::from(index_name)
        })
    }

    pub fn add(&mut self, value: LogValue, offset: u64) {
        // simply add to the in-memory index
        // it's flushed to disk on close
        self.mem_index.insert(value, offset);
    }

    #[allow(resolve_trait_on_defaulted_unit)]
    pub fn get(&mut self, value: &LogValue) -> Result<Vec<u64>, Box<Error>> {
        let mut in_memory = match self.mem_index.get_vec(value) {
            Some(v) => v.clone(),
            None => Vec::<u64>::new()
        };

        // sort the vector
        in_memory.sort_unstable();

        let on_disk = match self.term_map.get(value) {
            None => Vec::new(),
            Some(x) => {
                // read the array off the disk
                let rec: (LogValue, Vec<u64>) = from_slice(self.rec_file.read_at(*x)?.as_slice())?;

                rec.1
            }
        };

        // compute a union iterator for the two lists
        Ok(in_memory.iter()
            .merge_join_by(on_disk.iter(), |i,j| i.cmp(j))
            .map(|e| {
                match e {
                    Left(x) => *x,
                    Right(x) => *x,
                    Both(x,_) => *x
                }
        }).collect::<Vec<_>>())
    }

    /// Flushes the in-memory index to disk
    pub fn flush(&mut self) -> Result<(), Box<Error>> {
        if self.mem_index.len() == 0 {
            return Ok( () );
        }

        // create our new temporary RecordFile
        let tmp_file_path = self.dir_path.join(self.index_name.to_owned() + ".tmp_index");

        let tmp_rec_file_res = RecordFile::new(&tmp_file_path, FILE_HEADER);

        if let Err(e) = tmp_rec_file_res {
            error!("Could not create temporary index file: {}", e.to_string());
            return Err(e);
        }

        let mut tmp_rec_file = tmp_rec_file_res?;

        // we'd need some sort of lock around this
        // LOCK: START
        self.term_map.clear(); // kill this as we re-populate it below

        for rec in (&mut self.rec_file).into_iter() {
            debug!("Attempting to deserialized: {}", buf2string(&rec));

            // get our term and our locations from the file
            let (term, mut locs): (LogValue, Vec<u64>) = from_slice(&rec)?;

            debug!("Read term from disk: {}", term);

            if self.mem_index.contains_key(&term) {
                // go through each location in the in-memory index, and insert it into
                // the location we're going to write to disk if it's not already found
                for mem_loc in self.mem_index.get_vec(&term).unwrap() {
                    debug!("\tFound term in mem_index");

                    // didn't find this location in the list of locations
                    if let Err(loc) = locs.binary_search(mem_loc) {
                        debug!("\tDidn't find location, so inserting");
                        locs.insert(loc, *mem_loc);
                    }
                }

                debug!("\tRemoving term from mem_index: {}", term);
                self.mem_index.remove(&term); // remove it from the map
            }

            let rec = (&term, locs);
            let mut buf = to_vec(&rec)?;

            debug!("\tInserting term record into new file & term_map: {}", term);

            let loc = tmp_rec_file.append(&buf)?; // add the (term, locs) to our RecordFile
            self.term_map.insert(term.clone(), loc); // add the record location to our term map
        }

        // now we need to go through whatever is left in the in-memory map
        for (term, locs) in self.mem_index.iter_all() {
            let rec = (term, locs);
            let mut buf = to_vec(&rec)?;

            debug!("Inserting term record into new file & term_map: {}", term);
            debug!("Writing rec to file: {}", buf2string(&buf));

            let loc = tmp_rec_file.append(&buf)?; // add the (term, locs) to our RecordFile
            self.term_map.insert(term.clone(), loc); // add the record location to our term map
            // don't both deleting from mem_index as we'll clear the whole thing below
        }

        self.mem_index.clear(); // everything should be written to disk at this point

        // Switch the two files
        remove_file(&self.rec_file.file_path).unwrap();
        rename(tmp_file_path, &self.rec_file.file_path).unwrap();

        tmp_rec_file.file_path = self.rec_file.file_path.clone(); // update the file name
        self.rec_file = tmp_rec_file; // update the rec_file

        // LOCK: END

        return Ok( () )
    }
}

impl Drop for IndexFile {
    fn drop(&mut self) {
        debug!("Closing index {}", self.index_name);

        // flush the in-memory terms to disk
        self.flush().unwrap();

        let buff = to_vec(&self.term_map).unwrap();

        if let Err(e) = self.rec_file.fd.seek(SeekFrom::Start(self.rec_file.end_of_file)) {
            error!("Unable to seek to the end of the RecordFile: {}", e.to_string());
            return;
        }

        if let Err(e) = self.rec_file.fd.write(&buff) {
            error!("Error writing serialized term map to file: {}", e.to_string());
        }

        info!("Closed index: {}", self.index_name);
    }
}

//pub struct IndexFileIterator {
//    rec_iter: RecordFileIterator,
//}
//
//impl IntoIterator for IndexFile {
//    type Item = (LogValue, Vec<u64>); // just going with a tuple for now
//    type IntoIter = IndexFileIterator;
//
//    fn into_iter(self) -> Self::IntoIter {
//        IndexFileIterator{ rec_iter: (&mut self.rec_file).into_iter() }
//    }
//}
//
//impl Iterator for IndexFileIterator {
//    type Item = (LogValue, Vec<u64>); // just going with a tuple for now
//
//    fn next(&mut self) -> Option<Self::Item> {
//        let rec = self.rec_iter.next();
//
//        if rec.is_none() {
//            return None;
//        }
//
//        match deserialize(rec.unwrap()) {
//            Err(_) => None,
//            Ok(v) => Some(v)
//        }
//    }
//}


#[cfg(test)]
mod tests {
    use ::index_file::IndexFile;
    use ::log_value::LogValue;

    use std::path::Path;
    use serde_json::Number;
    use simple_logger;

    #[test]
    fn new_file_no_slash() {
        simple_logger::init().unwrap();  // this will panic on error
        IndexFile::new(Path::new("/tmp"), "id").unwrap();
    }

    #[test]
    fn add_flush() {
        simple_logger::init().unwrap();  // this will panic on error
        let mut index_file = IndexFile::new(Path::new("/tmp"), "id").unwrap();

        index_file.add(LogValue::Number(Number::from(7)), 24);
        index_file.add(LogValue::String(String::from("test")), 16);
    }


    #[test]
    fn double_flush() {
        simple_logger::init().unwrap();  // this will panic on error
        let mut index_file = IndexFile::new(Path::new("/tmp"), "id").unwrap();

        index_file.add(LogValue::Number(Number::from(7)), 24);

        debug!("First flush");

        index_file.flush();

        index_file.add(LogValue::String(String::from("test")), 16);

        debug!("Second flush");
    }

    #[test]
    fn get() {
        simple_logger::init().unwrap();  // this will panic on error
        let mut index_file = IndexFile::new(Path::new("/tmp"), "test").unwrap();

        index_file.add(LogValue::String(String::from("test")), 16);

        let ret = index_file.get(&LogValue::String(String::from("test"))).unwrap();

        assert_eq!(ret, [16]);
    }
}
