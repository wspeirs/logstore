extern crate serde_json;
extern crate byteorder;

use serde_json::Number;
use serde_json::Value as JsonValue;
use serde::{Deserialize, Serialize};
use byteorder::{LE, WriteBytesExt};

use std::fmt::{self, Debug, Display};
use std::hash::{Hash, Hasher};
use std::cmp::Ordering;

#[derive(Serialize, Deserialize, Clone)]
pub enum LogValue {
    Null,
    Bool(bool),
    Number(Number),
    String(String),
    Array(Vec<LogValue>)
    // we purposefully don't have nested JSON
}

impl LogValue {
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            &LogValue::Null => vec![0; 0], // no real good way to represent this
            &LogValue::Bool(b) => vec![if b { 0x01} else { 0x00 }],
            &LogValue::Number(ref n) => {
                let mut buff = vec![];

                buff.write_f64::<LE>(n.as_f64().unwrap());

                buff
            },
            &LogValue::String(ref s) => Vec::from(s.as_bytes()),
            &LogValue::Array(ref a) => {
                let mut buff = Vec::new();

                for mut x in a.iter() {
                    buff.append(&mut Vec::from(x.as_bytes()));
                }

                buff
            }
        }
    }
}

impl Debug for LogValue {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            LogValue::Null => {
                formatter.debug_tuple("Null").finish()
            }
            LogValue::Bool(v) => {
                formatter.debug_tuple("Bool").field(&v).finish()
            }
            LogValue::Number(ref v) => {
                Debug::fmt(v, formatter)
            }
            LogValue::String(ref v) => {
                formatter.debug_tuple("String").field(v).finish()
            }
            LogValue::Array(ref v) => {
                formatter.debug_tuple("Array").field(v).finish()
            }
        }
    }
}

impl Display for LogValue {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            LogValue::Null => {
                formatter.debug_tuple("Null").finish()
            }
            LogValue::Bool(v) => {
                formatter.debug_tuple("Bool").field(&v).finish()
            }
            LogValue::Number(ref v) => {
                Debug::fmt(v, formatter)
            }
            LogValue::String(ref v) => {
                formatter.debug_tuple("String").field(v).finish()
            }
            LogValue::Array(ref v) => {
                formatter.debug_tuple("Array").field(v).finish()
            }
        }
    }
}

//impl<'a> From<&'a JsonValue> for LogValue {
//    fn from(v: &JsonValue) -> LogValue {
impl From<JsonValue> for LogValue {
    fn from(v: JsonValue) -> LogValue {
        match v {
            JsonValue::Null => LogValue::Null,
            JsonValue::Bool(b) => LogValue::Bool(b),
            JsonValue::Number(n) => LogValue::Number(n),
            JsonValue::String(s) => LogValue::String(s),
            JsonValue::Array(a) => LogValue::Array(a.iter().map(|x| Self::from(x.to_owned())).collect::<Vec<_>>()),
            JsonValue::Object(_) => panic!("Cannot convert JSON object to LogValue")
        }
    }
}

impl <'a> From<&'a JsonValue> for LogValue {
    fn from(v: &JsonValue) -> LogValue {
        match v {
            &JsonValue::Null => LogValue::Null,
            &JsonValue::Bool(ref b) => LogValue::Bool(b.to_owned()),
            &JsonValue::Number(ref n) => LogValue::Number(n.to_owned()),
            &JsonValue::String(ref s) => LogValue::String(s.to_owned()),
            &JsonValue::Array(ref a) => {
                let ret = a.iter().map(|x| Self::from(x.to_owned())).collect::<Vec<_>>();
                LogValue::Array(ret)
            },
            &JsonValue::Object(_) => panic!("Cannot convert JSON object to LogValue")
        }
    }
}

impl PartialOrd for LogValue {
    fn partial_cmp(&self, other: &LogValue) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LogValue {
    fn cmp(&self, other: &LogValue) -> Ordering {
        match (self, other) {
            (&LogValue::Null, &LogValue::Null) => Ordering::Equal,
            (&LogValue::Bool(ref b1), &LogValue::Bool(ref b2)) => b1.cmp(&b2),
            (&LogValue::Number(ref n1), &LogValue::Number(ref n2)) => n1.as_f64().unwrap().partial_cmp(&n2.as_f64().unwrap()).unwrap(),
            (&LogValue::String(ref s1), &LogValue::String(ref s2)) => s1.cmp(&s2),
            (&LogValue::Array(ref a1), &LogValue::Array(ref a2)) => a1.cmp(&a2),
            (&LogValue::Null, _) => Ordering::Less,
            (&LogValue::Bool(_), _) => Ordering::Less,
            (&LogValue::Number(_), _) => Ordering::Less,
            (&LogValue::String(_), _) => Ordering::Less,
            (&LogValue::Array(_), _) => Ordering::Less,
        }
    }
}

impl Eq for LogValue { }

impl PartialEq for LogValue {
    fn eq(&self, other: &LogValue) -> bool {
        match (self, other) {
            (&LogValue::Null, &LogValue::Null) => true,
            (&LogValue::Bool(b1), &LogValue::Bool(b2)) => b1 == b2,
            (&LogValue::Number(ref n1), &LogValue::Number(ref n2)) => n1.as_f64() == n2.as_f64(),
            (&LogValue::String(ref s1), &LogValue::String(ref s2)) => s1 == s2,
            (&LogValue::Array(ref a1), &LogValue::Array(ref a2)) => a1 == a2,
            _ => false
        }
    }
}

impl Hash for LogValue {
    fn hash<H>(&self, state: &mut H) where H: Hasher {
        match self {
            &LogValue::Null => 0.hash(state),
            &LogValue::Bool(b) => b.hash(state),
            &LogValue::Number(ref n) => state.write_i64(n.as_f64().unwrap() as i64),
            &LogValue::String(ref s) => s.hash(state),
            &LogValue::Array(ref a) => a.hash(state)
        }
    }
}
