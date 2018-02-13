#[macro_use] extern crate log;
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate serde_json;

extern crate base64;
extern crate bytes;
extern crate byteorder;
extern crate futures;
extern crate futures_cpupool;
extern crate hyper;
extern crate itertools;
extern crate rmp_serde as rmps;
extern crate serde;
extern crate simple_logger;
extern crate twox_hash;
extern crate positioned_io;
extern crate rayon;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;

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
mod rpc_codec;
mod rpc_server;
mod record_error;
//mod http_server;


use std::collections::HashMap;
use std::path::Path;
use std::thread;
use log::Level;
use std::io::Error as IOError;
use std::rc::Rc;

use tokio_core::reactor::{Handle, Core};
use tokio_proto::{TcpServer, TcpClient, Connect};
use tokio_service::Service;
use futures::future;
use futures::sync::mpsc;
use futures::{Stream, stream, Future};
use futures::stream::IterOk;

use ::log_value::LogValue;
use ::utils::buf2string;
use ::json::json2map;
use ::log_file::LogFile;
use ::index_file::IndexFile;
use ::data_manager::DataManager;

use ::rpc_server::run_rpc_server;
//use ::http_server::configure_http_server;
use ::rpc_server::{RPCClient, MessageProto};
use ::rpc_codec::ResponseMessage;

extern crate time;
use time::PreciseTime;
use serde_json::Number;

fn main() {
    simple_logger::init_with_level(Level::Debug).unwrap();  // this will panic on error

    // create the core for the clients and HTTP Server
    let mut core = Core::new().unwrap();

    // construct the server info
    let mut server_info: HashMap<u32, RPCClient> = HashMap::new();

    // insert bogus servers
    server_info.insert(0, RPCClient::new("127.0.0.1:12345".to_string(), core.handle()));
    server_info.insert(1, RPCClient::new("127.0.0.1:2345".to_string(), core.handle()));

    // spaw off our RPC server
    let handler = thread::Builder::new().name("rpc server".to_string()).spawn(move || {
        run_rpc_server()
    }).unwrap();


//    let client_stream =
//        stream::iter_ok(["127.0.0.1:12345", "127.0.0.1:23456"].iter())
//        .and_then(|addr| {
//            let socket_addr = addr.parse().unwrap();
//            let handle = core.handle();
//
//            info!("Attempting to connect to {}", addr);
//
//            TcpClient::new(MessageProto).connect(&socket_addr, &handle).and_then(|client| Ok(client))
//        });

//    let connections = ["127.0.0.1:12345", "127.0.0.1:23456"].iter().map(|addr| {
//        let addr = "127.0.0.1:12345".parse().unwrap();
//        let handle = core.handle();
//
//        info!("Attempting to connect to {}", addr);
//
//        TcpClient::new(MessageProto).connect(&addr, &handle).and_then(|client| Ok(client))
//    }).collect::<Vec<_>>();
//
//    let client_stream: IterOk<_, IOError> = stream::iter_ok(connections.iter());
//
//    let clients = core.run(client_stream.into_future()).unwrap();



//    println!("{:?}", clients);

//    let http_core = core.handle();
//
//    debug!("GOT HERE");
//
//    configure_http_server(&http_core, clients);
//
//    core.run(future::empty::<(), ()>()).unwrap();
//    handler.join().unwrap();
}

