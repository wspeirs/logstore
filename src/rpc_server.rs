use std::str;
use std::io::{Cursor, Read, Write, ErrorKind, Error as IOError};
use std::collections::HashMap;

use byteorder::{LE, ReadBytesExt, WriteBytesExt};
use bytes::BytesMut;
use tokio_io::codec::{Encoder, Decoder};
use tokio_proto::pipeline::{ServerProto, ClientProto};
use tokio_proto::{TcpServer, TcpClient};
use tokio_service::Service;
use futures::{Stream, Sink, Future, future};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::Framed;
use tokio_core::reactor::Core;
use rmps::decode::from_read;
use rmps::encode::to_vec;

use ::log_value::LogValue;
use ::rpc_codec::{ServerCodec, ClientCodec};
use ::rpc_codec::{RequestMessage, ResponseMessage};

pub struct MessageProto;
pub struct RPCService;

impl<T: AsyncRead + AsyncWrite + 'static> ServerProto<T> for MessageProto {
    // For this protocol style, `Request` matches the `Item` type of the codec's `Decoder`
    type Request = RequestMessage;

    // For this protocol style, `Response` matches the `Item` type of the codec's `Encoder`
    type Response = ResponseMessage;

    // A bit of boilerplate to hook in the codec:
    type Transport = Framed<T, ServerCodec>;
    type BindTransport = Result<Self::Transport, IOError>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(ServerCodec::new()))
    }
}

impl<T: AsyncRead + AsyncWrite + 'static> ClientProto<T> for MessageProto {
    type Request = RequestMessage;
    type Response = ResponseMessage;

    type Transport = Framed<T, ClientCodec>;
    type BindTransport = Result<Self::Transport, IOError>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(ClientCodec::new()))
    }
}

impl Service for RPCService {
    // These types must match the corresponding protocol types:
    type Request = RequestMessage;
    type Response = ResponseMessage;

    // For non-streaming protocols, service errors are always io::Error
    type Error = IOError;

    // The future for computing the response; box it for simplicity.
    type Future = Box<Future<Item = Self::Response, Error =  Self::Error>>;

    // Produce a future for computing a response from a request.
    fn call(&self, req: Self::Request) -> Self::Future {
        debug!("Request: {:?}", req);

        Box::new(future::ok(ResponseMessage::Ok))
    }
}

pub fn run_server() {
    let addr = "0.0.0.0:12345".parse().unwrap();

    let server = TcpServer::new(MessageProto, addr);

    server.serve(|| Ok(RPCService));
}

pub fn run_client() {
    let addr = "127.0.0.1:12345".parse().unwrap();
    let mut core = Core::new().unwrap();

    let connection = TcpClient::new(MessageProto).connect(&addr, &core.handle());

    let client = connection.and_then(|client| {
        let req = RequestMessage::Get(String::from("hello"), LogValue::String(String::from("world")));

        client.call(req).and_then(move |response| {
            println!("RES: {:?}", response);
            Ok( () )
        })
    });

    core.run(client).unwrap();
}


#[cfg(test)]
mod tests {
    use ::rpc_server::run_client;

    #[test]
    fn test() {
        println!("Running client...");
        run_client();
    }
}