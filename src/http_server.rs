use futures;
use futures::future::Future;
use futures::Stream;

use hyper;
use hyper::{Method, StatusCode, Body, Chunk};
use hyper::header::ContentLength;
use hyper::server::{Http, Request, Response, Service};

struct Echo;

fn reverse(chunk: Chunk) -> Response {
    let reversed = chunk.iter().rev().cloned().collect::<Vec<u8>>();
    Response::new().with_body(reversed)
}

impl Service for Echo {
    // boilerplate hooking up hyper's server types
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    // The future representing the eventual Response your call will
    // resolve to. This can change to whatever Future you need.
    type Future = Box<Future<Item=Self::Response, Error=Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        match(req.method(), req.path()) {
                (&Method::Get, "/") => {
                    Box::new(futures::future::ok(Response::new().with_body("Try POSTING data")))
                },
                (&Method::Post, "/echo") => {
                    Box::new(req.body().concat2().map(reverse))
                },
                _ => {
                    Box::new(futures::future::ok(Response::new().with_status(StatusCode::NotFound)))
                }
        }
    }
}

pub fn run_server() {
    let addr = "127.0.0.1:3000".parse().unwrap();
    let server = Http::new().bind(&addr, || Ok(Echo)).unwrap();
    server.run().unwrap();
}