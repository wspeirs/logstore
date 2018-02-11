use futures;
use futures::future;
use futures::{Sink, Stream, Future};
use futures::sync::mpsc;

use hyper;
use hyper::{Method, StatusCode, Body, Chunk};
use hyper::error::Error;
use hyper::header::ContentLength;
use hyper::server::{Http, Request, Response, Service};
use tokio_core::reactor::{Handle, Core};

use rpc_server::run_client;

struct ElasticsearchService(Handle);

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
                let (mut tx, rx) = mpsc::channel(2);

                let rpc_future = run_client(&self.0, rx);

                tx.send(String::from("test")).wait();

                Box::new(rpc_future
                    .map_err(|e| hyper::Error::Io(e))
                    .map(|rpc_rsp| {
                        debug!("RPC RSP: {:?}", rpc_rsp);
                        let body: ResponseStream = Box::new(Body::from(NOTFOUND));
                        Response::new().with_body(body)
                }))
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

pub fn run_server() {
    let addr = "127.0.0.1:3000".parse().unwrap();
//    let server = Http::new().bind(&addr, || Ok(ElasticsearchService)).unwrap();
//    server.run().unwrap();

    let mut core = Core::new().unwrap();

    let http_handle = core.handle();
    let rpc_handle = core.handle();

    let serve = Http::new().serve_addr_handle(&addr, &http_handle, move || Ok(ElasticsearchService(rpc_handle.clone()))).unwrap();
    println!("Listening on http://{} with 1 thread.", serve.incoming_ref().local_addr());

    let http_handle_2 = http_handle.clone();

    http_handle.spawn(serve.for_each(move |conn| {
        http_handle_2.spawn(conn.map(|_| ()).map_err(|err| println!("serve error: {:?}", err)));
        Ok(())
    }).map_err(|_| ()));

    core.run(future::empty::<(), ()>()).unwrap();
}