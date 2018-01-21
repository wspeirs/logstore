#[macro_use] extern crate log;
#[macro_use] extern crate serde_json;

extern crate simple_logger;

#[macro_use] extern crate serde_derive;
extern crate serde;
extern crate rmp_serde as rmps;

use rmps::encode::to_vec;
use rmps::decode::from_slice;

extern crate base32;
extern crate base64;

use self::base32::Alphabet;
use base64::{Config, CharacterSet, LineWrap};

// my files/modules
mod utils;
mod log_file;
mod index_file;
mod log_value;
mod record_file;

use std::collections::HashMap;
use ::log_value::LogValue;
use ::utils::buf2string;


fn main() {
    simple_logger::init().unwrap();  // this will panic on error

//    let id = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF";
//
//    let ret_32 = base32::encode(Alphabet::RFC4648 { padding: false }, id);
//    println!("BASE32: {}", ret_32);
//
//    let ret_64 = base64::encode_config(id, Config::new(CharacterSet::Standard, false, true, LineWrap::NoWrap));
//    println!("BASE64: {}", ret_64);

//    let mut hm = HashMap::new();
//
//    hm.insert(LogValue::String(String::from("hello")), 34 as u64);
//    hm.insert(LogValue::String(String::from("world")), 0xABCDEF as u64);
//
//    println!("VEC: {}", buf2string(to_vec(&hm).unwrap().as_slice()));
//
//    let buff = b"\x82\x92\x03\x91\xA5\x77\x6F\x72\x6C\x64\xCE\x00\xAB\xCD\xEF\x92\x03\x91\xA5\x68\x65\x6C\x6C\x6F\x22";
//
//    assert_eq!(hm, from_slice(buff).unwrap());

    let lg = LogValue::String(String::from("test"));
    {
        let rec_ref = (&lg, vec![34, 64, 78]);
        println!("VEC REF: {}", buf2string(to_vec(&rec_ref).unwrap().as_slice()));
    }

    let rec = (lg, vec![34, 64, 78]);

    println!("    VEC: {}", buf2string(to_vec(&rec).unwrap().as_slice()));

    let buff = b"\x92\x92\x03\x91\xA4\x74\x65\x73\x74\x93\x22\x40\x4E";

    assert_eq!(rec, from_slice(buff).unwrap());
//    assert_eq!(hm, from_slice(buff).unwrap());
}
