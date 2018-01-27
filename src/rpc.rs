extern crate bytes;
extern crate futures;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;
extern crate rmp_serde as rmps;

use ::log_value::LogValue;

use std::io;
use std::str;
use std::io::Cursor;
use std::collections::HashMap;

use self::bytes::BytesMut;
use self::tokio_io::codec::{Encoder, Decoder};
use self::tokio_proto::pipeline::ServerProto;
use self::tokio_service::Service;
use self::futures::{future, Future};
use self::tokio_io::{AsyncRead, AsyncWrite};
use self::tokio_io::codec::Framed;

use rmps::decode::from_read;
use rmps::encode::to_vec;


pub struct MessageCodec;

#[derive(Serialize, Deserialize)]
pub enum Message {
    Insert(HashMap<String, LogValue>),
//    InsertAll(Vec<HashMap<String, LogValue>>),
    Get(String, LogValue)
}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Message>> {
        let cursor = Cursor::new(&buf);

        debug!("LEN: {}\tPOS: {}", buf.len(), cursor.position());

        let ret = from_read::<Cursor<_>, Message>(cursor);

        debug!("LEN: {}", buf.len());

        if let Err(e) = ret {
            debug!("Got error: {}", e.to_string());
            // assume this means we need more data?!?
            return Ok( None );
        } else {
            let msg = ret.unwrap();

            match msg {
                Message::Insert(i) => { debug!("GOT INSERT!!!"); return Ok(Some(Message::Insert(i))); },
                Message::Get(k,v) => { debug!("GET: {} {}", k, v); return Ok(Some(Message::Get(k,v))); }
            }

//            return Ok( Some( msg ) );
        }
    }
}

impl Encoder for MessageCodec {
    type Item = Message;
    type Error = io::Error;

    fn encode(&mut self, msg: Message, buf: &mut BytesMut) -> io::Result<()> {
        let msg_bytes = to_vec(&msg).unwrap();

        debug!("Encoding");

        buf.extend(msg_bytes.as_slice());

        Ok( () )
    }
}


pub struct MessageProto;

impl<T: AsyncRead + AsyncWrite + 'static> ServerProto<T> for MessageProto {
    // For this protocol style, `Request` matches the `Item` type of the codec's `Decoder`
    type Request = Message;

    // For this protocol style, `Response` matches the `Item` type of the codec's `Encoder`
    type Response = Message;

    // A bit of boilerplate to hook in the codec:
    type Transport = Framed<T, MessageCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(MessageCodec))
    }
}

pub struct MessageService;

impl Service for MessageService {
    // These types must match the corresponding protocol types:
    type Request = Message;
    type Response = Message;

    // For non-streaming protocols, service errors are always io::Error
    type Error = io::Error;

    // The future for computing the response; box it for simplicity.
    type Future = Box<Future<Item = Self::Response, Error =  Self::Error>>;

    // Produce a future for computing a response from a request.
    fn call(&self, req: Self::Request) -> Self::Future {
        debug!("In Call");

        // In this case, the response is immediate.
        Box::new(future::ok(req))
    }
}


#[cfg(test)]
mod tests {
    extern crate bytes;
    extern crate futures;
    extern crate tokio_io;
    extern crate tokio_core;
    extern crate tokio_proto;
    extern crate tokio_service;
    extern crate rmp_serde as rmps;

    use std::io;
    use std::net::ToSocketAddrs;
    use simple_logger;

    use self::futures::Future;
    use self::tokio_core::net::TcpStream;
    use self::tokio_core::reactor::Core;

    use ::rpc::Message::Get;
    use ::log_value::LogValue;
    use rmps::encode::to_vec;
    use rmps::decode::from_read;

    #[test]
    fn test() {
        simple_logger::init().unwrap();  // this will panic on error

        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let addr = "0.0.0.0:12345".to_socket_addrs().unwrap().next().unwrap();

        debug!("Trying to connect");

        let socket = TcpStream::connect(&addr, &handle);
        let msg = Get(String::from("key"), LogValue::String(String::from("value")));
        let buf = to_vec(&msg).unwrap();

        let request = socket.and_then(|s| {
            tokio_io::io::write_all(s, buf)
        });


//        let request = socket.and_then(|socket| {
//            debug!("Writing buffer");
//            tokio_io::io::write_all(socket, buf);
//        });

//        let response = request.and_then(|(socket, _request)| {
//            tokio_io::io::read_to_end(socket, Vec::new())
//        });

        let (_socket, data) = core.run(request).unwrap();
    }

}


