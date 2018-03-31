use futures;
use futures::future;
use futures::future::FutureResult;
use futures::{Future, Stream};
use futures::stream;

use std::io::{Error as IOError};

use hyper;
use hyper::{Body, Chunk, Method, StatusCode};
use hyper::error::Error;
use hyper::header::ContentLength;
use hyper::server::{Http, Request, Response, Service};
use tokio_core::reactor::Handle;

use rpc_server::RPCClient;
use rpc_codec::{RequestMessage, ResponseMessage};
use log_value::LogValue;
use json::{map2json, value2logvalue};
use serde_json::{Value, Map, from_slice};

use std::rc::Rc;
use std::collections::HashMap;
use std::thread;
use std::time;
use std::str::from_utf8;

struct ElasticsearchService(Rc<HashMap<u32, RPCClient>>);

pub type ResponseStream = Box<Stream<Item = Chunk, Error = Error>>;

static NOTFOUND: &[u8] = b"Not Found";
static VERSION_RESPONSE: &[u8] = br#"
{
  "name" : "logstore",
  "cluster_name" : "logstore",
  "version" : {
    "number" : "2.0.1",
    "build_hash" : "1ebe2f1ab149d45b08d5336b12eb14eb8a3814af",
    "build_timestamp" : "2015-11-18T17:42:23Z",
    "build_snapshot" : false,
    "lucene_version" : "5.2.1"
  },
  "tagline" : "You Know, for Search"
}"#;


fn parse_logs(log_chunk: Chunk) -> Box<Stream<Item=RequestMessage, Error=hyper::Error>> {
    let log_str = String::from_utf8(log_chunk.to_vec()).unwrap();

    let req_stream =
        log_str.lines().map(move |line| {
            let v: Value = from_slice(line.as_bytes()).unwrap();

            if !v.is_object() {
                warn!("Read non-object from _bulk POST: {}", line);
                return None;
            }

            let json_map = v.as_object().unwrap();

            // we want to skip the meta info
            // could use a counter for this as it's: meta, data, meta, etc...
            if json_map.contains_key("index") {
                return None;
            }

            // convert the JSON Map to a LogValue HashMap
            let log_value_map = value2logvalue(&json_map);

            // create the RPC request message
            Some(RequestMessage::Insert(log_value_map))
        });

    Box::new(stream::iter_ok(req_stream.filter(move |m| m.is_some()) // filter out the Nones
        .map(move |o| o.unwrap()))) // convert from Some(r) -> r
}

impl Service for ElasticsearchService {
    // boilerplate hooking up hyper's server types
    type Request = Request;
    type Response = Response<ResponseStream>;
    type Error = hyper::Error;

    // The future representing the eventual Response your call will resolve to
    type Future = Box<future::Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        let clients = self.0.clone();

        info!("HTTP REQUEST: {} {}", req.method(), req.path());

        match (req.method(), req.path()) {
            (&Method::Put, _) => {
                Box::new(futures::future::ok(
                    Response::new()
                        .with_status(StatusCode::Ok)
                        .with_header(ContentLength(0))
                    ,
                ))
            }

            (&Method::Post, _) => {
                let future_log_stream = req
                    .body()
                    .concat2()
                    .map(parse_logs);

                let response_stream =
                    future_log_stream.map(move |req_stream| {
                        req_stream.map(move |req| {
                            stream::futures_unordered(
                                clients.values().map(move |rpc_client| {
                                    rpc_client.make_request(req.clone()).map_err(|e| hyper::error::Error::Io(e))
                                })
                            )
                            }).flatten()
                    }).flatten_stream();

                let response =
                    response_stream.fold(Chunk::default(), move |c, r| future::ok::<Chunk, hyper::Error>(c));


                Box::new(response.map(|_| {
                    Response::new().with_status(StatusCode::NoContent)
                }))
            }

            (&Method::Get, "/") => {
                let body: ResponseStream = Box::new(Body::from(VERSION_RESPONSE));

                Box::new(futures::future::ok(
                    Response::new()
                        .with_status(StatusCode::Ok)
                        .with_header(ContentLength(VERSION_RESPONSE.len() as u64))
                        .with_body(body),
                ))
//                let response_futures =
//                    clients.values().map(move |rpc_client| {
//                    // make a bogus request, would come from GET request
//                    let req = RequestMessage::Get(
//                        String::from("method"),
//                        LogValue::String(String::from("GET")),
//                    );
//
//                    rpc_client.make_request(req)
//                });
//
//                // accumulate all the results together
//                let response = stream::futures_unordered(response_futures)
//                    .map_err(|e| { debug!("** ERROR ** {:?}", e); Error::Io(e) })
//                    .map(|resp| {
//                        debug!("RSP: {:?}", resp);
//
//                        match resp {
//                            ResponseMessage::Ok => stream::iter_ok(vec![]),
//                            ResponseMessage::Logs(l) => stream::iter_ok(l.into_iter().map(|m| Chunk::from(map2json(m).to_string())).collect::<Vec<_>>())
//                        }
//                    })
//                    .flatten()
//                ;
//
//                let body: ResponseStream = Box::new(response);
//
//                Box::new(futures::future::ok( Response::new()
//                    .with_status(StatusCode::Ok)
//                    .with_body(body)
//                ))
            }
            _ => {
                let body: ResponseStream = Box::new(Body::from(NOTFOUND));

                Box::new(futures::future::ok(
                    Response::new()
                        .with_status(StatusCode::NotFound)
                        .with_header(ContentLength(NOTFOUND.len() as u64))
                        .with_body(body),
                ))
            }
        }
    }
}

pub fn configure_http_server(handle: &Handle, clients: HashMap<u32, RPCClient>) {
    let addr = "127.0.0.1:9200".parse().unwrap();
    let rc = Rc::new(clients);

    let serve = Http::new()
        .serve_addr_handle(&addr, &handle, move || Ok(ElasticsearchService(rc.clone())))
        .unwrap();

    println!(
        "Listening on http://{} with 1 thread.",
        serve.incoming_ref().local_addr()
    );

    let http_handle_2 = handle.clone();

    handle.spawn(
        serve
            .for_each(move |conn| {
                http_handle_2.spawn(
                    conn.map(|_| ())
                        .map_err(|err| println!("serve error: {:?}", err)),
                );
                Ok(())
            })
            .map_err(|_| ()),
    );
}
