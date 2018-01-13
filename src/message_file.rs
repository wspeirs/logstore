extern crate serde_json;

use self::serde_json::{Value, Map};
use self::serde_json::Error as JsonError;
use self::serde_json::error::ErrorCode;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, ErrorKind, Seek, SeekFrom};
use std::io::Error as IOError;

/// This struct represents the on-disk format of the MessageFile
/// The messages follow the VV, where VV is the version number:
/// |-------------------------------------------|
/// | 0x4C 0x4F 0x47 0x53 | 0x54 0x4F 0x52 0xVV |
/// | L    O    G    S    | T    O    R    0xVV |
/// |-------------------------------------------|
/// | records in the file, -1 = not saved       |
/// |-------------------------------------------|
/// | Tombstone: 0 or 1   | Message size        |
/// |-------------------------------------------|
/// | message in JSON                           |
/// |-------------------------------------------|
/// | ...                                       |
/// |-------------------------------------------|

// this is kinda clunky :-\
fn make_json_error(msg: &str) -> JsonError {
    return JsonError::syntax(ErrorCode::Message(String::from(msg).into_boxed_str()), 0, 0)
}


///
/// The message file that holds all of the log messages
///
pub struct MessageFile {
    fd: File, // the actual message file
}

impl MessageFile {
    pub fn new(file_path: &str) -> Result<MessageFile, Box<Error>> {
        let mut msg_file_path = String::from(file_path);

        if !msg_file_path.ends_with("/") {
            msg_file_path.push_str("/messages.data");
        } else {
            msg_file_path.push_str("messsages.data");
        }

        trace!("Attempting to open message file: {}", msg_file_path);

        // open the file, and return if an error is encountered
        let msg_file = OpenOptions::new().read(true).write(true).create(true).open(&msg_file_path)?;

        debug!("Created MessageFile with {}", msg_file_path);

        // create our structure and return it wrapped in an Ok()
        return Ok(MessageFile { fd: msg_file });
    }

    pub fn add_message(&mut self, message: &str) -> Result<(), Box<Error>> {
        // attempt to parse the JSON
        let v:Value = serde_json::from_str(message)?;

        // unpack the resulting value, and ensure it's an object
        let json:Map<String, Value> = match v {
            Value::Object(x) => Ok(x),
            _ => Err(make_json_error("Messages must be JSON objects"))
        }.unwrap();

        // check to see if there are any restricted fields
        // TODO: Need to check for nested objects
        if json.keys().any(|k| k.starts_with("__")) {
            return Err(From::from("Illegal fields in message; fields cannot start with __: ".to_owned() + message));
        }

        // get a sorted vector of keys to form a canonical representation
        let mut sorted_keys = json.keys().collect::<Vec<&String>>();
        sorted_keys.sort_unstable();

        let mut canoncial_json = String::from("{");

        // go through each sorted key, and convert it into a string based upon type
        // or return an error if a nested JSON object is found
        for key in sorted_keys.into_iter() {
            let value = match json.get(key).unwrap().to_owned() {
                Value::Object(_) => Err(make_json_error("Nested JSON Objects are not allowed")),
                Value::Null => Ok(String::from("")),
                Value::Bool(b) => Ok(b.to_string()),
                Value::Number(n) => Ok(n.to_string()),
                Value::String(s) => Ok(s),
                Value::Array(v) => Ok(v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(","))
            }?;

            // add key: value
            canoncial_json.push_str(&format!("{}:{},", key.to_owned(), value));
        }

        canoncial_json.pop(); // remove trailing comma
        canoncial_json.push_str("}");

        println!("{}", canoncial_json);

        // write the data to the file, matching on the error
        match self.fd.write(&message.as_bytes()) {
            Ok(_) => Ok( () ),
            Err(e) => Err(From::from(e))
        }
    }
}

#[cfg(test)]
mod tests {
    use ::message_file::MessageFile;

    #[test]
    fn new_file_no_slash() {
        let ret = MessageFile::new("/tmp").unwrap();
    }

    #[test]
    fn new_file_with_slash() {
        let ret = MessageFile::new("/tmp/").unwrap();
    }

    #[test]
    fn write_message() {
        let mut ret = MessageFile::new("/tmp").unwrap();

        ret.add_message("{\"b\": 1, \"a\": \"something\"}");
    }
}