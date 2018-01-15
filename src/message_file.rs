extern crate serde_json;
extern crate twox_hash;
extern crate base32;
extern crate time;
extern crate byteorder;

use serde_json::{Value, Map};
use serde_json::Error as JsonError;
use serde_json::error::ErrorCode;
use self::twox_hash::XxHash;
use self::base32::Alphabet;
use self::byteorder::{LE, ReadBytesExt, WriteBytesExt};

use std::str;
use std::io::Cursor;
use std::hash::Hasher;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};

// this is kinda clunky :-\
fn make_json_error(msg: &str) -> JsonError {
    return JsonError::syntax(ErrorCode::Message(String::from(msg).into_boxed_str()), 0, 0);
}

fn get_ts() -> (i64) {
    let ts = time::get_time();

    return (ts.sec * 1000) + (ts.nsec as i64 / 1000000);
}

///
/// The message file that holds all of the log messages
///
pub struct MessageFile {
    fd: File, // the actual message file
    num_messages: u32, // the number of messages in the file
    //TODO: Need to add a destructor as this value is never saved
}

/// This struct represents the on-disk format of the MessageFile
/// The messages follow the VV, where VV is the version number and
/// 0xTT is the if the message has been tombstoned:
/// |-------------------------------------------|
/// | 0x4C 0x4F 0x47 0x53 | 0x54 0x4F 0x52 0xVV |
/// | L    O    G    S    | T    O    R    0xVV |
/// |-------------------------------------------|
/// | num msgs in file    | 0xTT | msg size ... |
/// |-------------------------------------------|
/// | ... | message  in JSON ...                |
/// |-------------------------------------------|
/// | ... message in JSON                       |
/// |-------------------------------------------|
/// | ...                                       |
/// |-------------------------------------------|

const VERSION: u8 = 0x1;
const NUM_MSG_OFFSET: u64 = 8;
const BAD_MSG_COUNT: u32 = 0xFFFFFFFF;

impl MessageFile {
    /// Creates a new MessageFile and initializes the header
    pub fn new(file_path: &str) -> Result<MessageFile, Box<Error>> {
        let mut msg_file_path = String::from(file_path);

        if !msg_file_path.ends_with("/") {
            msg_file_path.push_str("/messages.data");
        } else {
            msg_file_path.push_str("messages.data");
        }

        trace!("Attempting to open message file: {}", msg_file_path);

        // open the file, and return if an error is encountered
        let mut msg_file = OpenOptions::new().read(true).write(true).create(true).open(&msg_file_path)?;
        let mut num_messages = 0;

        msg_file.seek(SeekFrom::Start(0))?; // make sure we're at the start of the file

        // if we're opening a new file, write the header info
        if msg_file.metadata()?.len() == 0 {
            msg_file.write(b"LOGSTOR")?; // write the header/magic
            msg_file.write_u8(VERSION)?; // write the version
            msg_file.write_u32::<LE>(BAD_MSG_COUNT)?; // indicate this file wasn't closed properly
            info!("Created new MessageFile: {}", msg_file_path);
        } else {
            let mut magic = vec![0; 7];

            msg_file.read_exact(&mut magic)?;

            if "LOGSTOR".as_bytes() != magic.as_slice() {
                return Err(From::from("Invalid magic header for file"));
            }

            let version = msg_file.read_u8()?;

            if VERSION != version {
                return Err(From::from("Wrong version number"));
            }

            num_messages = msg_file.read_u32::<LE>()?;
            msg_file.seek(SeekFrom::End(0))?; // go to the end of the file

            info!("Opened MessageFile: {}", msg_file_path);
        }

        let mut ret = MessageFile{
            fd: msg_file,
            num_messages: num_messages
        };

        if num_messages == BAD_MSG_COUNT {
            error!("MessageFile not properly closed, attempting to check file");

            return match ret.check() {
                Ok(count) => { info!("Read {} messages from file successfully", count); Ok(ret) },
                Err(e) => Err(From::from(e))
            };
        } else {
            debug!("Returning MessageFile");
            return Ok(ret);
        }
   }

    ///
    /// Checks the file attempting to read each JSON message, and re-establish the count
    ///
    pub fn check(&mut self) -> Result<(u32), Box<Error>> {
        let mut count = 0;

        for msg_buff in self.into_iter() {
            let msg = str::from_utf8(&msg_buff[5..]).unwrap();

            debug!("Read message: {}", msg);

            let res: Result<Value, JsonError> = serde_json::from_str(msg);

            if let Err(e) = res {
                warn!("Error parsing message into JSON: {}", e.to_string());
                return Err(From::from(e));
            }

            count += 1;
        }

        self.num_messages = count;

        Ok(count)
    }

    fn canonicalize_json() { }

    ///
    /// Adds a record to the file
    ///
    pub fn add(&mut self, message: &str) -> Result<String, Box<Error>> {
        trace!("Attempting to parse JSON: {}", message);

        let v: Value = serde_json::from_str(message)?;

        trace!("Parsed JSON");

        // unpack the resulting value, and ensure it's an object
        let mut json: Map<String, Value> = match v {
            Value::Object(x) => Ok(x),
            _ => Err(make_json_error("Messages must be JSON objects"))
        }.unwrap();

        trace!("Checking all fields for __");

        let mut canoncial_json = String::from("{");

        { // this wraps all the immutable borrows
            // check to see if there are any restricted fields
            if json.keys().any(|k| k.starts_with("__")) {
                return Err(From::from("Illegal fields in message; fields cannot start with __: ".to_owned() + message));
            }

            // add the TS to the message
            json.insert(String::from("__ts"), json!(get_ts()));

            // get a sorted vector of keys to form a canonical representation
            let mut sorted_keys = json.keys().collect::<Vec<&String>>();

            sorted_keys.sort_unstable();

            // go through each sorted key, and convert it into a string based upon type
            // or return an error if a nested JSON object is found
            for key in sorted_keys.into_iter() {
                let value = match json.get(key).unwrap().to_owned() {
                    Value::Object(_) => Err(make_json_error("Nested JSON Objects are not allowed")),
                    Value::Null => Ok(String::from("null")),
                    Value::Bool(b) => Ok(b.to_string()),
                    Value::Number(n) => Ok(n.to_string()),
                    Value::String(s) => Ok(format!("'{}'", s)),
                    //TODO: Need to canoncialize this too
                    Value::Array(v) => Ok(v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(","))
                }?;

                // add key: value
                canoncial_json.push_str(&format!("{}:{},", key.to_owned(), value));
            }
        }

        canoncial_json.pop(); // remove trailing comma
        canoncial_json.push_str("}");

        debug!("Canonical: {}", canoncial_json);
        println!("Canonical: {}", canoncial_json);

        // construct our hash function
        let mut hash: XxHash = XxHash::with_seed(0xBEDBEEF);

        hash.write(canoncial_json.as_bytes());

        let mut buff = vec![];

        buff.write_u64::<LE>(hash.finish())?;

        let id = base32::encode(Alphabet::RFC4648 { padding: false }, &buff);

        println!("ID: {}", id);

        // add the ID to the message
        json.insert(String::from("__id"), Value::String(id.to_owned()));

        let final_message = serde_json::to_string(&json)?;

        println!("Final Message: {} {}", final_message.len(), final_message);

        // create a buffer to perform our writes to
        let mut msg_buff = vec![];

        // write out that we're not tombstoning this
        msg_buff.write_u8(0x00)?;

        // write out the size of the message
        msg_buff.write_u32::<LE>(final_message.len() as u32)?;

        // write the message
        msg_buff.write(&final_message.as_bytes())?;

        // write everything in one write call
        self.fd.write(&msg_buff)?;

        // flush to disk
        self.fd.flush()?;

        // update the number of messages written
        self.num_messages += 1;

        return Ok(id.to_owned());
    }

    pub fn tombstone(&mut self, id: &str) -> Result<bool, Box<Error>> {
        let mut prev_pos = NUM_MSG_OFFSET + 4; // start of the messages
        let mut found = false;

        for mut block in self.into_iter() {
            let mut cur = Cursor::new(block);
            cur.seek(SeekFrom::Start(1))?; // move past the tombstone

            let msg_size = cur.read_u32::<LE>().unwrap() as u64;
            block = cur.into_inner();

            let message = str::from_utf8(&block[5..])?;

            let json = match serde_json::from_str(&message) {
                Err(e) => {
                    warn!("Error parsing message into JSON: {}", e.to_string());
                    debug!("MSG: {}", message);
                    return Err(From::from(e));
                },
                Ok(x) => match x {
                    Value::Object(o) => o,
                    _ => {
                        warn!("Found non-object JSON: {}", message);
                        return Err(From::from("Found non-object JSON"));
                    }
                }
            };

            if json.get("__id").unwrap() == id {
                debug!("Found id to tombstone");
                found = true;
                break;
            }

            prev_pos += 5 + msg_size;
        }

        if found {
            // go back to the start of the other message
            self.fd.seek(SeekFrom::Start(prev_pos))?;

            debug!("Tombstoning at {}", prev_pos);

            // write out that we want to tombstone it
            self.fd.write_u8(0x01)?;

            // go to the end of the file
            self.fd.seek(SeekFrom::End(0))?;

            return Ok(true);
        } else {
            return Ok(false); // we weren't able to find it
        }
    }
}

impl Drop for MessageFile {
    fn drop(&mut self) {
        self.fd.seek(SeekFrom::Start(NUM_MSG_OFFSET)).unwrap();
        self.fd.write_u32::<LE>(self.num_messages).unwrap(); // cannot return an error, so best attempt
        self.fd.flush().unwrap();

        debug!("Wrote out the number of messages: {}", self.num_messages);
    }
}

pub struct MessageFileIterator<'a> {
    msg_file: &'a mut MessageFile,
    msg_file_size: u64
}

impl <'a> IntoIterator for &'a mut MessageFile {
    type Item = Vec<u8>;
    type IntoIter = MessageFileIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        // move to the beginning of the messages
        self.fd.seek(SeekFrom::Start(NUM_MSG_OFFSET + 4)).unwrap();

        // get the size of the file
        let file_size = self.fd.metadata().unwrap().len();

        debug!("Created MessageFileIterator");

        MessageFileIterator{
            msg_file: self,
            msg_file_size: file_size
        }
    }
}

impl <'a> Iterator for MessageFileIterator<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let cur_pos = self.msg_file.fd.seek(SeekFrom::Current(0)).unwrap();

        if cur_pos == self.msg_file_size {
            return None;
        }

        let mut msg_buff = Vec::<u8>::with_capacity(4096); // make a large buffer
        msg_buff.resize(5, 0x00); // TOODO: use resize_default()

        match self.msg_file.fd.read_exact(&mut msg_buff[0..5]) {
            Err(e) => { warn!("Error reading MessageFile header: {}", e.to_string()); return None; },
            _ => { }
        }

        let mut msg_header_cursor = Cursor::new(msg_buff);

        let tombstone = match msg_header_cursor.read_u8() {
            Err(e) => { warn!("Error tombstone: {}", e.to_string()); return None; },
            Ok(v) => v
        };

        debug!("Got tombstone: {}", tombstone);

        let msg_size = match msg_header_cursor.read_u32::<LE>() {
            Err(e) => { warn!("Error reading message size: {}", e.to_string()); return None; },
            Ok(v) => v
        };

        // make sure we have enough space
        msg_buff = msg_header_cursor.into_inner();
        msg_buff.resize(5 + msg_size as usize, 0x00); // TODO: use resize_default()

        debug!("Reading message of size {}", msg_size);

        match self.msg_file.fd.read_exact(&mut msg_buff[5..]) {
            Err(e) => { warn!("Error reading message ({}): {}", msg_size, e.to_string()); return None; },
            _ => { }
        }

        Some(msg_buff)
    }
}

#[cfg(test)]
mod tests {
    use ::message_file::MessageFile;
    use simple_logger;


    #[test]
    fn new_file_no_slash() {
        simple_logger::init().unwrap();  // this will panic on error
        MessageFile::new("/tmp").unwrap();
    }

    #[test]
    fn new_file_with_slash() {
        simple_logger::init().unwrap();  // this will panic on error
        MessageFile::new("/tmp/").unwrap();
    }

    #[test]
    fn check_file() {
        simple_logger::init().unwrap();  // this will panic on error
        let mut msg_file = MessageFile::new("/tmp/").unwrap();

        assert!(msg_file.num_messages == msg_file.check().unwrap());
    }

    #[test]
    fn add_valid_msg() {
        simple_logger::init().unwrap();  // this will panic on error
        let mut msg_file = MessageFile::new("/tmp").unwrap();
        let msg = r#"{
            "d": 23,
            "c": null,
            "b": true,
            "a": "something"
        }"#;

        let id = msg_file.add(msg).unwrap();

        println!("ID: {}", id);
    }

    #[test]
    fn add_nested_json() {
        simple_logger::init().unwrap();  // this will panic on error
        let mut msg_file = MessageFile::new("/tmp").unwrap();
        let msg = r#"{
            "c": "test",
            "b": 23,
            "a": { "x": "z" }
        }"#;

        // this should be an error
        assert!(msg_file.add(msg).is_err());
    }

    #[test]
    fn add_illegal_field() {
        simple_logger::init().unwrap();  // this will panic on error
        let mut msg_file = MessageFile::new("/tmp").unwrap();
        let msg = r#"{
            "__c": "test",
            "b": 23,
            "a": true
        }"#;

        // this should be an error
        assert!(msg_file.add(msg).is_err());
    }

    #[test]
    fn tombstone_message() {
        simple_logger::init().unwrap();  // this will panic on error
        let mut msg_file = MessageFile::new("/tmp").unwrap();
        let msg = r#"{
            "z": "test"
        }"#;
        let id = msg_file.add(msg).unwrap();

        assert!(msg_file.tombstone(id.as_str()).unwrap());
    }
}