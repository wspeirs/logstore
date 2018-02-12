use futures;
use futures::future;
use futures::{Sink, Stream, Future};
use futures::sync::mpsc;
use futures::stream;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::sync::mpsc::Sender;

use hyper;
use hyper::{Method, StatusCode, Body, Chunk};
use hyper::error::Error;
use hyper::header::ContentLength;
use hyper::server::{Http, Request, Response, Service};
use tokio_core::reactor::{Handle, Core};
use tokio_proto::Connect;
use tokio_proto::pipeline::Pipeline;

use rpc_server::{RPCClient, MessageProto};
use rpc_server::{connect_rpc_client};
use ::rpc_codec::{ResponseMessage, RequestMessage};
use ::log_value::LogValue;

use std::rc::Rc;
use std::io::Error as IOError;
use std::borrow::Borrow;

struct ElasticsearchService(Rc<Vec<RPCClient>>);

pub type ResponseStream = Box<Stream<Item=Chunk, Error=Error>>;

static NOTFOUND: &[u8] = b"Not Found";

impl Service for ElasticsearchService {
    // boilerplate hooking up hyper's server types
    type Request = Request;
    type Response = Response<ResponseStream>;
    type Error = hyper::Error;

    // The future representing the eventual Response your call will resolve to
    type Future = Box<future::Future<Item=Self::Response, Error=Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        match(req.method(), req.path()) {
            (&Method::Get, "/") => {

                let all_sends = self.0.iter().map(move |rpc_client: &RPCClient| {
                    let client = rpc_client.clone();

                    let req = RequestMessage::Get(String::from("method"), LogValue::String(String::from("GET")));
                    let send = client.tx.send(req);

                    *client.res
                });

//                let results = stream::futures_unordered(all_sends);
//                    .and_then(|resp| {
//                        println!("RSP: {:?}", resp);
//                    }).collect();

                let body: ResponseStream = Box::new(Body::from(NOTFOUND));

                Box::new(futures::future::ok(Response::new()
                    .with_status(StatusCode::NotFound)
                    .with_header(ContentLength(NOTFOUND.len() as u64))
                    .with_body(body)
                ))

//                Box::new(rpc_future
//                    .map_err(|e| hyper::Error::Io(e))
//                    .map(|rpc_rsp| {
//                        debug!("RPC RSP: {:?}", rpc_rsp);
//                        let body: ResponseStream = Box::new(Body::from(NOTFOUND));
//                        Response::new().with_body(body)
//                }))
            },
            _ => {
                let body: ResponseStream = Box::new(Body::from(NOTFOUND));

                Box::new(futures::future::ok(Response::new()
                        .with_status(StatusCode::NotFound)
                        .with_header(ContentLength(NOTFOUND.len() as u64))
                        .with_body(body)
                ))
            }
        }
    }
}

pub fn configure_http_server(handle: &Handle, clients: Vec<RPCClient>) {
    let addr = "127.0.0.1:3000".parse().unwrap();
    let rc = Rc::new(clients);

    let serve =
        Http::new().serve_addr_handle(&addr, &handle, move || Ok(ElasticsearchService(rc.clone()))).unwrap();
    println!("Listening on http://{} with 1 thread.", serve.incoming_ref().local_addr());

    let http_handle_2 = handle.clone();

    handle.spawn(serve.for_each(move |conn| {
        http_handle_2.spawn(conn.map(|_| ()).map_err(|err| println!("serve error: {:?}", err)));
        Ok(())
    }).map_err(|_| ()));
}