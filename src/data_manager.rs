use std::collections::HashMap;
use std::error::Error;
use std::io::{Error as IOError, ErrorKind};
use std::path::{Path, PathBuf};
use std::fs::read_dir;

use ::log_file::LogFile;
use ::index_file::IndexFile;
use ::log_value::LogValue;

pub struct DataManager {
    log_file: LogFile,
    indices: HashMap<String, IndexFile>,
    dir_path: PathBuf
}

impl DataManager {
    pub fn new(dir_path: &Path) -> Result<DataManager, Box<Error>> {

        // make sure we're passed a directory
        if !dir_path.is_dir() {
            return Err(Box::new(IOError::new(ErrorKind::InvalidInput, format!("{} is not a directory", dir_path.display()))));
        }

        let log_file = LogFile::new(dir_path)?;
        let mut indices = HashMap::<String, IndexFile>::new();

        info!("Loading files from: {}", dir_path.display());

        // look for any index files in this directory
        for entry in read_dir(dir_path)? {
            match entry {
                Err(e) => return Err(Box::new(e)),
                Ok(f) => {
                    let path = f.path();

                    if path.is_file() && path.extension().is_some() && path.extension().unwrap() == "index" {
                        let index_name = String::from(path.file_stem().unwrap().to_str().unwrap());

                        info!("Loading index file: {}", path.display());

                        indices.insert(index_name.to_owned(), IndexFile::new(&dir_path, index_name.as_str())?);
                    }
                }
            }
        }

        Ok( DataManager{ log_file, indices, dir_path: PathBuf::from(dir_path) })
    }

    pub fn insert(&mut self, log: &HashMap<String, LogValue>) -> Result<(), Box<Error>> {
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

    pub fn get(&mut self, key: &str, value: &LogValue) -> Result<Vec<HashMap<String, LogValue>>, Box<Error>> {
        // get the locations from the index, or return if the key is not found
        let locs = match self.indices.get_mut(key) {
            Some(i) => i.get(value)?,
            None => return Ok(Vec::new())
        };

        // create the vector to return all the log entires
        let mut ret = Vec::<HashMap<String, LogValue>>::with_capacity(locs.len());

        // go through the record file fetching the records
        for loc in locs {
            ret.push(self.log_file.get(loc)?);
        }

        Ok(ret)
    }

    pub fn flush(&mut self) -> () {
        for val in self.indices.values_mut() {
            val.flush();
        }
    }
}