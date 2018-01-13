//#[macro_use]
//extern crate log;

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
}