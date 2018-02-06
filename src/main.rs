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
extern crate lru_cache;
extern crate positioned_io;

extern crate bytes;
extern crate futures;
extern crate futures_cpupool;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;
extern crate tokio_core;

use rmps::encode::to_vec;
use rmps::decode::from_slice;

// my files/modules
mod utils;
mod log_file;
mod index_file;
mod log_value;
mod record_file;
mod record_error;
mod json;
mod data_manager;
mod rpc_codec;
mod rpc_server;

use tokio_proto::TcpServer;


use std::collections::HashMap;
use std::path::Path;

use ::log_value::LogValue;
use ::utils::buf2string;
use ::json::json2map;
use ::log_file::LogFile;
use ::index_file::IndexFile;
use ::data_manager::DataManager;

use ::rpc_server::run_server;

extern crate time;
use time::PreciseTime;
use serde_json::Number;

fn main() {
    simple_logger::init().unwrap();  // this will panic on error

    run_server();

}

