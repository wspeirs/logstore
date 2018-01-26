#[macro_use] extern crate log;
#[macro_use] extern crate serde_json;
#[macro_use] extern crate serde_derive;
extern crate base64;
extern crate byteorder;
extern crate itertools;
extern crate rmp_serde as rmps;
extern crate serde;
extern crate simple_logger;
extern crate twox_hash;

use rmps::encode::to_vec;
use rmps::decode::from_slice;

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

extern crate time;
use time::PreciseTime;
use serde_json::Number;

fn main() {
//    simple_logger::init().unwrap();  // this will panic on error

    let mut data_manager = DataManager::new(Path::new("/tmp")).unwrap();

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

    for i in 0..10000 {
        log.insert(String::from("count"), LogValue::Number(Number::from(i)));
        data_manager.insert(&log).unwrap();
    }

    let end_insert1 = PreciseTime::now();

    println!("{} seconds for 10K inserts", start.to(end_insert1));

    for i in 0..100 {
        data_manager.get("count", &LogValue::Number(Number::from(i))).unwrap();
    }

    let end_get1 = PreciseTime::now();

    println!("{} seconds for 100 gets", end_insert1.to(end_get1));

    data_manager.flush();

    let end_flush = PreciseTime::now();

    println!("{} seconds for flush", end_get1.to(end_flush));

    for i in 0..10000 {
        log.insert(String::from("count"), LogValue::Number(Number::from(i))).unwrap();
        data_manager.insert(&log);
    }

    let end_insert2 = PreciseTime::now();

    println!("{} seconds for insert", end_flush.to(end_insert2));

//    for i in 0..100 {
//        data_manager.get("host", &LogValue::String(String::from("localhost"))).unwrap();
//    }
//
//    let end_get2 = PreciseTime::now();
//
//    println!("{} seconds for 100 gets", end_insert2.to(end_get2));


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
