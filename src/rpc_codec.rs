/*
 * Copied from https://github.com/jgallagher/tokio-chat-example
 */
use serde::{Serialize, Deserialize};
use tokio_io::codec::{Encoder, Decoder};
use bytes::BytesMut;
use bytes::IntoBuf;
use bytes::Buf;
use byteorder::{LE, ReadBytesExt, WriteBytesExt};
use rmps::decode::from_slice;
use rmps::encode::to_vec;

use std::io::{Cursor, ErrorKind, Error as IOError, Result as IOResult};
use std::marker::PhantomData;
use std::collections::HashMap;

use ::log_value::LogValue;

pub struct LengthPrefixedMessage<Recv, Send> {
    _recv: PhantomData<Recv>,
    _send: PhantomData<Send>
}

impl <Recv, Send> LengthPrefixedMessage<Recv, Send> {
    pub fn new() -> LengthPrefixedMessage<Recv, Send> {
        LengthPrefixedMessage{_recv: PhantomData, _send: PhantomData}
    }
}

// `LengthPrefixedMessage` is a codec for sending and receiving MessagePack serializable types. The
// over the wire format is a Little Endian u32 indicating the number of bytes in the payload
// (not including the 4 u32 bytes themselves) followed by the MessagePack payload.
impl<Recv, Send> Decoder for LengthPrefixedMessage<Recv, Send> where for<'de> Recv: Deserialize<'de> {
    type Item = Recv;
    type Error = IOError;

    fn decode(&mut self, buf: &mut BytesMut) -> IOResult<Option<Self::Item>> {
        let buf_len = buf.len();

        if buf_len < 4 {
            return Ok(None);
        }

        let mut size_buf : [u8; 4] = [0; 4];
        size_buf.clone_from_slice(&buf[0..4]);

        let mut cursor = Cursor::new(size_buf);

        // read in the size, indicate we need more bytes if it fails
        let msg_size = match cursor.read_u32::<LE>() {
            Ok(msg_size) => msg_size,
            Err(_) => return Ok(None),
        };

        debug!("DECODE: SIZE: {} + 4\tBUF LEN: {}", msg_size, buf.len());

        // Make sure our buffer has all the bytes indicated by msg_size + 4 bytes for the size
        if buf.len() < (msg_size + 4) as usize {
            debug!("INDICATING WE NEED MORE BYTES");
            return Ok(None);
        }

        buf.split_to(4 as usize);

        let msg_buf: BytesMut = buf.split_to(msg_size as usize);

        debug!("GOT BUFFER OF SIZE: {}", msg_buf.len());

        let ret: Recv = from_slice(&msg_buf[..]).map_err(|err| IOError::new(ErrorKind::InvalidData, err))?;

        Ok(Some(ret))
    }
}

impl<Recv, Send> Encoder for LengthPrefixedMessage<Recv, Send> where Send: Serialize {
    type Item = Send;
    type Error = IOError;

    fn encode(&mut self, msg: Send, buf: &mut BytesMut) -> IOResult<()> {
        let msg_bytes = to_vec(&msg).unwrap();
        let msg_size = msg_bytes.len() as u32;
        let mut msg_size_buf = vec![];

        debug!("ENCODE SIZE: {}", msg_size);

        msg_size_buf.write_u32::<LE>(msg_size)?;

        buf.extend_from_slice(&msg_size_buf);
        buf.extend_from_slice(&msg_bytes);

        Ok( () )
    }
}

// create the two message enums
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum RequestMessage {
    Insert(HashMap<String, LogValue>),
    //    InsertAll(Vec<HashMap<String, LogValue>>),
    Get(String, LogValue)
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum ResponseMessage {
    Ok, // response to Insert and InsertAll
    Logs(Vec<HashMap<String, LogValue>>) // response to Get
}


// setup the codecs for each type
pub type ServerCodec = LengthPrefixedMessage<RequestMessage, ResponseMessage>;
pub type ClientCodec = LengthPrefixedMessage<ResponseMessage, RequestMessage>;


#[cfg(test)]
mod tests {
    use ::rpc_codec::{RequestMessage, ResponseMessage};
    use ::rpc_codec::{ServerCodec, ClientCodec};
    use ::log_value::LogValue;

    use byteorder::{LE, ReadBytesExt, WriteBytesExt};
    use bytes::BytesMut;
    use std::io::Cursor;
    use rmps::encode::to_vec;
    use std::io::Write;
    use tokio_io::codec::{Decoder, Encoder};

    #[test]
    fn decode_server_test() {
        let buff = Vec::new();
        let request = RequestMessage::Get(String::from("hello"), LogValue::String(String::from("world")));
        let ser = to_vec(&request).unwrap();
        let mut cursor = Cursor::new(buff);

        cursor.write_u32::<LE>(ser.len() as u32).unwrap();
        cursor.write_all(ser.as_slice());

        let mut codec = ServerCodec::new();

        let mut byte_mut = BytesMut::new();

        byte_mut.extend_from_slice(cursor.get_ref());

        println!("LEN BEFORE: {}", byte_mut.len());

        let res = codec.decode(&mut byte_mut).unwrap();

        assert_eq!(byte_mut.len(), 0); // ensure we consumed the whole buffer

        match res {
            None => panic!("Returned None"),
            Some(r) => assert_eq!(request, r)
        };
    }

    #[test]
    fn decode_client_test() {
        let buff = Vec::new();
        let request = ResponseMessage::Ok;
        let ser = to_vec(&request).unwrap();
        let mut cursor = Cursor::new(buff);

        cursor.write_u32::<LE>(ser.len() as u32).unwrap();
        cursor.write_all(ser.as_slice());

        let mut codec = ClientCodec::new();

        let mut byte_mut = BytesMut::new();

        byte_mut.extend_from_slice(cursor.get_ref());

        println!("LEN BEFORE: {}", byte_mut.len());

        let res = codec.decode(&mut byte_mut).unwrap();

        assert_eq!(byte_mut.len(), 0); // ensure we consumed the whole buffer

        match res {
            None => panic!("Returned None"),
            Some(r) => assert_eq!(request, r)
        };
    }

    #[test]
    fn encode_server_test() {
        let response = ResponseMessage::Ok;

        let mut byte_mut = BytesMut::new();
        let mut codec = ServerCodec::new();

        codec.encode(response, &mut byte_mut).unwrap();

        assert_ne!(byte_mut.len(), 0); // ensure we wrote something to the buffer
    }

    #[test]
    fn encode_client_test() {
        let response = RequestMessage::Get(String::from("hello"), LogValue::String(String::from("world")));

        let mut byte_mut = BytesMut::new();
        let mut codec = ClientCodec::new();

        codec.encode(response, &mut byte_mut).unwrap();

        assert_ne!(byte_mut.len(), 0); // ensure we wrote something to the buffer
    }
}