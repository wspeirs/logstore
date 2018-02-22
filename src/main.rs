#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;

extern crate base64;
extern crate byteorder;
extern crate bytes;
extern crate futures;
extern crate futures_cpupool;
extern crate hyper;
extern crate itertools;
extern crate positioned_io;
extern crate rayon;
extern crate rmp_serde as rmps;
extern crate serde;
extern crate simple_logger;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;
extern crate twox_hash;

// my files/modules
mod utils;
mod log_file;
mod index_file;
mod log_value;
mod record_file;
mod json;
mod data_manager;
mod rpc_codec;
mod rpc_server;
mod record_error;
mod http_server;

use std::collections::HashMap;
use std::thread;
use std::time;
use log::Level;

use tokio_core::reactor::Core;
use futures::future;

use rpc_server::run_rpc_server;
use http_server::configure_http_server;
use rpc_server::RPCClient;

fn main() {
    simple_logger::init_with_level(Level::Debug).unwrap(); // this will panic on error

    // create the core for the clients and HTTP Server
    let mut core = Core::new().unwrap();

    // spaw off our RPC server
    let handler = thread::Builder::new()
        .name("rpc server".to_string())
        .spawn(move || run_rpc_server())
        .unwrap();

    // hackie
    thread::sleep(time::Duration::from_secs(5));

    debug!("Creating client map");

    // construct the server info
    let mut server_info: HashMap<u32, RPCClient> = HashMap::new();

    // insert bogus servers
    server_info.insert(
        0,
        RPCClient::new("127.0.0.1:12345".to_string(), &mut core),
    );
//    server_info.insert(
//        1,
//        RPCClient::new("127.0.0.1:2345".to_string(), &mut core),
//    );


    let http_core = core.handle();

    //    debug!("GOT HERE");

    configure_http_server(&http_core, server_info);

    core.run(future::empty::<(), ()>()).unwrap();
    handler.join().unwrap();
}
