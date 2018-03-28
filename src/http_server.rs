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


//fn parse_logs<T>(log_chunk: Chunk) -> FutureResult<T, hyper::Error> where T: Stream<Item=RequestMessage, Error=hyper::Error> {
//fn parse_logs(log_chunk: Chunk) -> FutureResult<Box<Stream<Item=RequestMessage, Error=hyper::Error>>, hyper::Error> {
fn parse_logs(log_chunk: Chunk) -> Box<Stream<Item=RequestMessage, Error=hyper::Error>> {
    let req_stream = stream::iter_ok(
        log_chunk.split(|c| *c == 10).map(|line| {
            let v: Value = from_slice(line).unwrap();

            if !v.is_object() {
                warn!("Read non-object from _bulk POST: {}", from_utf8(line).unwrap());
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
            return Some(RequestMessage::Insert(log_value_map));

        }).filter(|m| m.is_some()) // filter out the Nones
        .map(|o| o.unwrap())); // convert from Some(r) -> r

//    return future::ok::<T, hyper::Error>(req_stream);
//    return future::ok(Box::new(req_stream));
    return Box::new(req_stream);
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
                let request_stream = req
                    .body()
                    .concat2()
//                    .and_then(parse_logs);
                    .map(parse_logs);

                let response =
                    request_stream.map(|req_stream| {
                        let x =
                            req_stream.map(|req| {
                                clients.values().map(move |rpc_client| {
                                    rpc_client.make_request(req)
                                }).map_err(|e| {
                                    debug!("** ERROR ** {:?}", e);
                                    Error::Io(e)
                                }).map(|resp| {
                                    debug!("RSP: {:?}", resp);

                                    match resp {
                                        ResponseMessage::Ok => stream::iter_ok(vec![]),
                                        ResponseMessage::Logs(l) => stream::iter_ok(l.into_iter().map(|m| Chunk::from(map2json(m).to_string())).collect::<Vec<_>>())
                                    }
                                })
                            });
                        x
                    });

//                Box::new(req.body().concat2().map(|b| {
//                        for line in b.split(|c| *c == 10) {
//                            let v: Value = from_slice(line).unwrap();
//
//                            if !v.is_object() {
//                                warn!("Read non-object from _bulk POST: {}", from_utf8(line).unwrap());
//                                continue;
//                            }
//
//                            let json_map = v.as_object().unwrap();
//
//                            // we want to skip the meta info
//                            // could use a counter for this as it's: meta, data, meta, etc...
//                            if json_map.contains_key("index") {
//                                continue;
//                            }
//
//                            // convert the JSON Map to a LogValue HashMap
//                            let log_value_map = value2logvalue(&json_map);
//
//                            // create the RPC request message
//                            let req = RequestMessage::Insert(log_value_map);
//
//                            let response_futures =
//                                clients.values().map(move |rpc_client| {
//                                    rpc_client.make_request(req)
//                                });
//
//                            // accumulate all the results together
//                            let response = stream::futures_unordered(response_futures)
//                                .map_err(|e| { debug!("** ERROR ** {:?}", e); Error::Io(e) })
//                                .map(|resp| {
//                                    debug!("RSP: {:?}", resp);
//
//                                    match resp {
//                                        ResponseMessage::Ok => stream::iter_ok(vec![]),
//                                        ResponseMessage::Logs(l) => stream::iter_ok(l.into_iter().map(|m| Chunk::from(map2json(m).to_string())).collect::<Vec<_>>())
//                                    }
//                                })
//                                .flatten()
//                            ;
//
                            let body: ResponseStream = Box::new(response);

                            Box::new(futures::future::ok( Response::new()
                                .with_status(StatusCode::Ok)
                                .with_body(body)
                            ))
//                        }
//                        Response::new()
//                        .with_status(StatusCode::UnprocessableEntity)
//                    })
//                )


//                if &path[..6] == "/_bulk" {
//                    info!("GOT _bulk: {}", path);
//                    Box::new(futures::future::ok(
//                        Response::new()
//                            .with_status(StatusCode::Ok)
//                            .with_header(ContentLength(0))
//                        ,
//                    ))
//                } else {
//                    info!("Unknown POST: {}", path[..6].to_string());
//                    let body: ResponseStream = Box::new(Body::from(NOTFOUND));
//
//                    Box::new(futures::future::ok(
//                        Response::new()
//                            .with_status(StatusCode::NotFound)
//                            .with_header(ContentLength(NOTFOUND.len() as u64))
//                            .with_body(body),
//                    ))
//                }
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
