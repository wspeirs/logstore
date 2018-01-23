#[macro_use] extern crate log;
#[macro_use] extern crate serde_json;

extern crate simple_logger;

#[macro_use] extern crate serde_derive;
extern crate serde;
extern crate rmp_serde as rmps;
extern crate twox_hash;
extern crate byteorder;

use rmps::encode::to_vec;
use rmps::decode::from_slice;

extern crate base32;
extern crate base64;

use self::base32::Alphabet;
use base64::{Config, CharacterSet, LineWrap};

// my files/modules
mod utils;
mod log_file;
mod index_file;
mod log_value;
mod record_file;
mod json;
mod data_manager;

use std::collections::HashMap;
use std::path::Path;

use ::log_value::LogValue;
use ::utils::buf2string;
use ::json::json2map;
use ::log_file::LogFile;
use ::index_file::IndexFile;
use ::data_manager::DataManager;

fn main() {
    simple_logger::init().unwrap();  // this will panic on error

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

    let log = json2map(&json_str.to_string()).unwrap();
    let mut log_file = LogFile::new(Path::new("/tmp/")).unwrap();
    let mut req_index_file = IndexFile::new(Path::new("/tmp/"), "request").unwrap();

    let loc = log_file.add(&log).unwrap();

    req_index_file.add(log.get("request").unwrap().to_owned(), loc);


//    let id = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF";
//
//    let ret_32 = base32::encode(Alphabet::RFC4648 { padding: false }, id);
//    println!("BASE32: {}", ret_32);
//
//    let ret_64 = base64::encode_config(id, Config::new(CharacterSet::Standard, false, true, LineWrap::NoWrap));
//    println!("BASE64: {}", ret_64);

//    let mut hm = HashMap::new();
//
//    hm.insert(LogValue::String(String::from("hello")), 34 as u64);
//    hm.insert(LogValue::String(String::from("world")), 0xABCDEF as u64);
//
//    println!("VEC: {}", buf2string(to_vec(&hm).unwrap().as_slice()));
//
//    let buff = b"\x82\x92\x03\x91\xA5\x77\x6F\x72\x6C\x64\xCE\x00\xAB\xCD\xEF\x92\x03\x91\xA5\x68\x65\x6C\x6C\x6F\x22";
//
//    assert_eq!(hm, from_slice(buff).unwrap());


}
