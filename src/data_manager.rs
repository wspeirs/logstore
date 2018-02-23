use std::collections::HashMap;
use std::io::{Error as IOError, ErrorKind};
use std::path::{Path, PathBuf};
use std::fs::read_dir;
use rayon::prelude::*;

use ::log_file::LogFile;
use ::index_file::IndexFile;
use ::log_value::LogValue;
use ::record_error::RecordError;

pub struct DataManager {
    log_file: LogFile,
    indices: HashMap<String, IndexFile>,
    dir_path: PathBuf
}

impl DataManager {
    pub fn new(dir_path: &Path) -> Result<DataManager, RecordError> {

        // make sure we're passed a directory
        if !dir_path.is_dir() {
            let io_err = IOError::new(ErrorKind::InvalidInput, format!("{} is not a directory", dir_path.display()));
            return Err(RecordError::from(io_err));
        }

        let log_file = LogFile::new(dir_path)?;
        let mut indices = HashMap::<String, IndexFile>::new();

        info!("Loading files from: {}", dir_path.display());

        // look for any index files in this directory
        for entry in read_dir(dir_path).map_err(|e| RecordError::from(e))? {
            let file = entry.map_err(|e| RecordError::from(e))?;
            let path = file.path();

            if path.is_file() && path.extension().is_some() && path.extension().unwrap() == "index" {
                let index_name = String::from(path.file_stem().unwrap().to_str().unwrap());

                info!("Loading index file: {}", path.display());

                indices.insert(index_name.to_owned(), IndexFile::new(&dir_path, index_name.as_str())?);
            }
        }

        Ok( DataManager{ log_file, indices, dir_path: PathBuf::from(dir_path) })
    }

    pub fn insert(&mut self, log: &HashMap<String, LogValue>) -> Result<(), RecordError> {
        // add to the log file first
        let loc = self.log_file.add(log)?;

        // go through each key and create or add to index
        for (key, value) in log.iter() {
            if !self.indices.contains_key(key) {
                self.indices.insert(key.to_owned(), IndexFile::new(&self.dir_path, key)?);
            }

            let mut index_file = self.indices.get_mut(key).unwrap();

            index_file.add(value.to_owned(), loc);
        }

        Ok( () )
    }

    pub fn get(&mut self, key: &str, value: &LogValue) -> Result<Vec<HashMap<String, LogValue>>, RecordError> {
        // get the locations from the index, or return if the key is not found
        let locs: Vec<u64> = match self.indices.get_mut(key) {
            Some(i) => i.get(value)?,
            None => return Ok(Vec::new())
        };

        let log_file = &self.log_file;

        // fetch the records
        let ret :Result<Vec<HashMap<String, LogValue>>, RecordError> = locs.into_par_iter().map(|loc| {
            log_file.get(loc) //TODO: figure out how to throw error here
        }).collect();

        ret
    }

/*
    pub fn get_serial(&mut self, key: &str, value: &LogValue) -> Result<Vec<HashMap<String, LogValue>>, RecordError> {
        // get the locations from the index, or return if the key is not found
        let locs = match self.indices.get_mut(key) {
            Some(i) => i.get(value)?,
            None => return Ok(Vec::new())
        };

        let ret = locs.into_iter().map(|loc| {
            self.log_file.get(loc).unwrap()
        }).collect();

        Ok(ret)
    }
*/

    pub fn flush(&mut self) -> () {
        for val in self.indices.values_mut() {
            val.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use data_manager::DataManager;
    use log_value::LogValue;
    use serde_json::Number;
    use json::json2map;

    #[test]
    fn insert_test() {
        let json_str = json!({
        "time":"[11/Aug/2014:17:21:45 +0000]",
        "remoteIP":"127.0.0.1",
        "host":"localhost",
        "request":"/index.html",
        "query":"",
        "method":"GET",
        "status":"200",
        "userAgent":"ApacheBench/2.3",
        "referer":"-"
        });

        let mut log = json2map(&json_str.to_string()).unwrap();
        let mut dm = DataManager::new(Path::new("/tmp/")).unwrap();

        for i in 0..50 {
            log.insert(String::from("count"), LogValue::Number(Number::from(i)));
            dm.insert(&log).unwrap();
        }

    }

}

