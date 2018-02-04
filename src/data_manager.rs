use std::collections::HashMap;
use std::io::{Error as IOError, ErrorKind};
use std::path::{Path, PathBuf};
use std::fs::read_dir;
use std::cell::RefCell;
use lru_cache::LruCache;
use rayon::prelude::*;
use rayon::iter::Map;

use ::log_file::LogFile;
use ::index_file::IndexFile;
use ::log_value::LogValue;
use ::record_error::RecordError;

pub struct DataManager {
    log_file: LogFile,
    indices: HashMap<String, IndexFile>,
    dir_path: PathBuf,
    cache: LruCache<u64, HashMap<String, LogValue>>
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

        let cache = LruCache::new(100001);

        Ok( DataManager{ log_file, indices, dir_path: PathBuf::from(dir_path), cache })
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

        let cache = RefCell::new(&mut self.cache);
        let log_file = &self.log_file;

        // split into 2 iterators: those in the cache, those we need to read from disk
        let (in_mem, on_disk): (Vec<_>, Vec<_>) = locs.into_iter().partition(|loc| {
            cache.borrow_mut().contains_key(loc)
        });

        // fetch the on-disk ones
        let ret :Vec<(u64, HashMap<String, LogValue>)>= on_disk.into_par_iter().map(|loc| {
            (loc, log_file.get(loc).unwrap())
        }).collect();

        println!("ON DISK: {}", ret.len());

        // add them to our cache and strip out the location
        let it1 = ret.into_iter().map(|(loc, log)| {
            cache.borrow_mut().insert(loc, log.clone());
            log
        });

        // get our cached ones
        let it2 = in_mem.into_iter().map(|loc| {
            cache.borrow_mut().get_mut(&loc).unwrap().clone()
        });


        Ok(it2.chain(it1).collect::<Vec<_>>())
    }

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

    pub fn flush(&mut self) -> () {
        for val in self.indices.values_mut() {
            val.flush();
        }
    }
}


#[cfg(test)]
mod tests {
    use time::PreciseTime;
    use serde_json::Number;
    use std::path::Path;
    use rayon::{Configuration, initialize};

    use ::data_manager::DataManager;
    use ::log_value::LogValue;
    use ::json::json2map;

    fn do_inserts(dm: &mut DataManager, num_logs: usize) {
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

        println!("Starting inserts...");

        let start = PreciseTime::now();

        for i in 0..num_logs {
            log.insert(String::from("count"), LogValue::Number(Number::from(i)));
            dm.insert(&log).unwrap();
        }

        let end_insert1 = PreciseTime::now();

        println!("{} for {} inserts", start.to(end_insert1), num_logs);
    }

    #[test]
    fn test_get_parallel() {
//        initialize(Configuration::new().num_threads(32));

        let mut data_manager = DataManager::new(Path::new("/tmp")).unwrap();

        do_inserts(&mut data_manager, 100000);

        let start = PreciseTime::now();

        for i in 0..5 {
            let start = PreciseTime::now();

            data_manager.get("host", &LogValue::String(String::from("localhost"))).unwrap();

            let end = PreciseTime::now();

            println!("{} time for 1 get", start.to(end));
        }

        let end = PreciseTime::now();

        println!("{} for 100 gets", start.to(end));

    }

    #[test]
    fn test_get() {
        let mut data_manager = DataManager::new(Path::new("/tmp")).unwrap();

        do_inserts(&mut data_manager, 100000);

        let start = PreciseTime::now();

        for i in 0..5 {
            let start = PreciseTime::now();

            data_manager.get("host", &LogValue::String(String::from("localhost"))).unwrap();

            let end = PreciseTime::now();

            println!("{} time for 1 get", start.to(end));
        }

        let end = PreciseTime::now();

        println!("{} for 100 gets", start.to(end));

    }
}
