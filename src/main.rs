#[macro_use] extern crate log;
#[macro_use] extern crate serde_json;
extern crate simple_logger;

extern crate base32;
extern crate base64;

use self::base32::Alphabet;
//use base64::{encode, decode};
use base64::{Config, CharacterSet, LineWrap};

// my files/modules
//mod message_file;
//mod index_file;
//mod log_value;
mod record_file;


fn main() {
    simple_logger::init().unwrap();  // this will panic on error

    let id = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF";

    let ret_32 = base32::encode(Alphabet::RFC4648 { padding: false }, id);
    println!("BASE32: {}", ret_32);

    let ret_64 = base64::encode_config(id, Config::new(CharacterSet::Standard, false, true, LineWrap::NoWrap));
    println!("BASE64: {}", ret_64);
}
