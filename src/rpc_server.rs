use std::str;
use std::io::{Cursor, Read, Write, ErrorKind, Error as IOError};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use byteorder::{LE, ReadBytesExt, WriteBytesExt};
use bytes::BytesMut;
use tokio_io::codec::{Encoder, Decoder};
use tokio_proto::pipeline::{ServerProto, ClientProto};
use tokio_proto::{TcpServer, TcpClient};
use tokio_service::Service;
use futures::{Stream, Sink, Future, future};
use futures::sync::mpsc;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::Framed;
use tokio_core::reactor::Core;
use rmps::decode::from_read;
use rmps::encode::to_vec;

use ::log_value::LogValue;
use ::data_manager::DataManager;
use ::rpc_codec::{ServerCodec, ClientCodec};
use ::rpc_codec::{RequestMessage, ResponseMessage};

pub struct MessageProto;

pub struct RPCService {
    data_manager: Arc<Mutex<DataManager>>
}

impl RPCService {
    pub fn new(data_manager: Arc<Mutex<DataManager>>) -> RPCService {
        RPCService{ data_manager: data_manager }
    }
}

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

        let ret = match req {
            RequestMessage::Insert(log) => self.data_manager.lock().unwrap().insert(&log).map(|()| ResponseMessage::Ok),
            RequestMessage::Get(key, value) => self.data_manager.lock().unwrap().get(&key, &value).map(|v|ResponseMessage::Logs(v))
        }.map_err(|e| IOError::new(ErrorKind::InvalidData, format!("Error: {}", e.to_string())));

        Box::new(future::result(ret))
    }
}

pub fn run_server() {
    let addr = "0.0.0.0:12345".parse().unwrap();

    let server = TcpServer::new(MessageProto, addr);

    let dm = Arc::new(Mutex::new(DataManager::new(Path::new("/tmp")).unwrap()));

    server.serve(move || Ok(RPCService::new(dm.clone())));
}

pub fn run_client(rx: mpsc::Receiver<String>) {
    let addr = "127.0.0.1:12345".parse().unwrap();
    let mut core = Core::new().unwrap();

    let connection = TcpClient::new(MessageProto).connect(&addr, &core.handle());

    let run = connection.and_then(|client| {
        let req = RequestMessage::Get(String::from("method"), LogValue::String(String::from("GET")));

        rx.and_then(|msg| {
            println!("MSG: {}", msg);
            client.call(req).and_then(move |response| {
                println!("RES: {:?}", response);
                Ok( () )
            });
        });
    });

    core.run(run).unwrap();
}


#[cfg(test)]
mod tests {
    use ::rpc_server::run_client;

    #[test]
    fn test() {
        println!("Running client...");

        let (tx, rx) = mpsc::channel(2);

        run_client(rx);
    }
}