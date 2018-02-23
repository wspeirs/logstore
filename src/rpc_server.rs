use std::io::{Error as IOError, ErrorKind};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::boxed::Box;

use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::Framed;
use tokio_proto::{TcpClient, TcpServer};
use tokio_proto::pipeline::{ClientService, ClientProto, ServerProto};
use tokio_service::Service;
use futures::{future, Future};

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
                .map(|v| {
                    debug!("LOG: {:?}", v);
                    ResponseMessage::Logs(v)
                }),
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

    debug!("Starting RPC server");

    server.serve(move || Ok(RPCService::new(dm.clone())));
}

type Connection = ClientService<TcpStream, MessageProto>;

pub struct RPCClient {
    address: String,
    conn: Connection
}

impl RPCClient {
    pub fn new(address: String, core: &mut Core) -> RPCClient {
        let socket_addr = address.parse().unwrap();

        // create a handle for the connection
        let handle = core.handle();

        let connection_future = TcpClient::new(MessageProto).connect(&socket_addr, &handle);

        // establish this connection
        let conn = core.run(connection_future).unwrap();

        RPCClient {
            address,
            conn
        }
    }

    pub fn make_request(&self, req: RequestMessage) -> Box<Future<Item=ResponseMessage, Error=IOError>> {
        return Box::new(self.conn.call(req));
    }
}

#[cfg(test)]
mod tests {
    use rpc_server::{run_rpc_server};
    use std::{thread, time};
    use futures::sync::mpsc;
    use futures::Sink;

    #[test]
    fn test_server() {
        run_rpc_server();
    }
}
