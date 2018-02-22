use futures;
use futures::future;
use futures::{Future, Stream};
use futures::stream;

use hyper;
use hyper::{Body, Chunk, Method, StatusCode};
use hyper::error::Error;
use hyper::header::ContentLength;
use hyper::server::{Http, Request, Response, Service};
use tokio_core::reactor::Handle;

use rpc_server::RPCClient;
use rpc_codec::{RequestMessage, ResponseMessage};
use log_value::LogValue;
use json::map2json;

use std::rc::Rc;
use std::collections::HashMap;

struct ElasticsearchService(Rc<HashMap<u32, RPCClient>>);

pub type ResponseStream = Box<Stream<Item = Chunk, Error = Error>>;

static NOTFOUND: &[u8] = b"Not Found";

impl Service for ElasticsearchService {
    // boilerplate hooking up hyper's server types
    type Request = Request;
    type Response = Response<ResponseStream>;
    type Error = hyper::Error;

    // The future representing the eventual Response your call will resolve to
    type Future = Box<future::Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        let clients = self.0.clone();

        match (req.method(), req.path()) {
            (&Method::Get, "/") => {
                let response_futures =
                    clients.values().map(move |rpc_client| {
                    // make a bogus request, would come from GET request
                    let req = RequestMessage::Get(
                        String::from("method"),
                        LogValue::String(String::from("GET")),
                    );

                    rpc_client.make_request(req)
                });

                // accumulate all the results together
                let response = stream::futures_unordered(response_futures)
                    .map_err(|e| Error::Io(e))
                    .map(|resp| {
                        debug!("RSP: {:?}", resp);

                        match resp {
                            ResponseMessage::Ok => stream::iter_ok(vec![]),
                            ResponseMessage::Logs(l) => stream::iter_ok(l.into_iter().map(|m| Chunk::from(map2json(m).to_string())).collect::<Vec<_>>())
                        }
                    })
                    .flatten()
                ;

                let body: ResponseStream = Box::new(response);

                Box::new(futures::future::ok( Response::new()
                    .with_status(StatusCode::Ok)
                    .with_body(body)
                ))
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
    let addr = "127.0.0.1:3000".parse().unwrap();
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
