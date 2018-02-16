use std::str;
use std::io::{Cursor, Error as IOError, ErrorKind, Read, Write};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::cell::{Ref, RefCell};
use std::boxed::Box;
use std::rc::Rc;

use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use bytes::BytesMut;
use tokio_core::reactor::{Core, Handle};
use tokio_core::net::{TcpStream, TcpStreamNew};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::{Decoder, Encoder, Framed};
use tokio_proto::{BindClient, Connect, TcpClient, TcpServer};
use tokio_proto::pipeline::{ClientProto, Pipeline, ServerProto};
use tokio_service::Service;
use futures::{future, Future, Sink, Stream};
use futures::sync::mpsc;
use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::IntoFuture;
use rmps::decode::from_read;
use rmps::encode::to_vec;

use log_value::LogValue;
use data_manager::DataManager;
use rpc_codec::{ClientCodec, ServerCodec};
use rpc_codec::{RequestMessage, ResponseMessage};

pub struct MessageProto;

pub struct RPCService {
    data_manager: Arc<Mutex<DataManager>>,
}

impl RPCService {
    pub fn new(data_manager: Arc<Mutex<DataManager>>) -> RPCService {
        RPCService {
            data_manager: data_manager,
        }
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
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    // Produce a future for computing a response from a request.
    fn call(&self, req: Self::Request) -> Self::Future {
        debug!("Request: {:?}", req);

        let ret = match req {
            RequestMessage::Insert(log) => self.data_manager
                .lock()
                .unwrap()
                .insert(&log)
                .map(|()| ResponseMessage::Ok),
            RequestMessage::Get(key, value) => self.data_manager
                .lock()
                .unwrap()
                .get(&key, &value)
                .map(|v| ResponseMessage::Logs(v)),
        }.map_err(|e| {
            IOError::new(ErrorKind::InvalidData, format!("Error: {}", e.to_string()))
        });

        Box::new(future::result(ret))
    }
}

pub fn run_rpc_server() {
    let addr = "0.0.0.0:12345".parse().unwrap();

    let server = TcpServer::new(MessageProto, addr);

    let dm = Arc::new(Mutex::new(DataManager::new(Path::new("/tmp")).unwrap()));

    server.serve(move || Ok(RPCService::new(dm.clone())));
}

pub fn connect_rpc_client(handle: &Handle) -> Connect<Pipeline, MessageProto> {
    let addr = "127.0.0.1:12345".parse().unwrap();

    TcpClient::new(MessageProto).connect(&addr, &handle)
}

//type Connection = Box<Future<Item = Connect<Pipeline, MessageProto>, Error = IOError>>;

//pub enum RPCConnection {
//    Disconnected,
//    Connected(Connection),
//}

pub struct RPCClient {
    address: String,
    tx: Sender<RequestMessage>,
    rx: Receiver<ResponseMessage>,
}

impl RPCClient {
    pub fn new(address: String, handle: Handle) -> RPCClient {
        let socket_addr = address.parse().unwrap();

        // create a sender -> RCPClient channel
        let (mut send2rpc_tx, send2rpc_rx) = mpsc::channel(2);

        // create a RPCClient -> receiver channel
        let (mut rpc2rcv_tx, rpc2rcv_rx) = mpsc::channel(2);

        let connection_future = TcpClient::new(MessageProto)
            .connect(&socket_addr, &handle)
            .and_then(move |client| {
                send2rpc_rx
                    .map_err(|e| unreachable!("rx can't fail"))
                    .and_then(move |msg: RequestMessage| {
                        println!("REQUEST: {:?}", msg);

                        client.call(msg).and_then(|response: ResponseMessage| {
                            println!("RESPONSE: {:?}", response);
                            rpc2rcv_tx
                                .clone()
                                .send(response)
                                .map_err(|e| IOError::new(ErrorKind::InvalidData, e.to_string()))
                        })
                    })
                    .fold((), |_acc, _| Ok::<(), IOError>(()))
            });

        RPCClient {
            address,
            tx: send2rpc_tx,
            rx: rpc2rcv_rx,
        }
    }

    pub fn get_connection(&self) -> (Sender<RequestMessage>, Receiver<ResponseMessage>) {
        (self.tx.clone(), self.rx)
    }
}

#[cfg(test)]
mod tests {
    use rpc_server::{make_request, run_rpc_server};
    use std::{thread, time};
    use futures::sync::mpsc;
    use futures::Sink;

    #[test]
    fn test_server() {
        run_rpc_server();
    }

    //    #[test]
    //    fn test_client() {
    //        println!("Running client...");
    //
    //        let (mut tx, rx) = mpsc::channel(2);
    //
    //        tx.start_send(String::from("test")).unwrap();
    //        tx.poll_complete().unwrap();
    //
    //        println!("Calling run_client");
    //
    //        run_client(rx);
    //    }
}
