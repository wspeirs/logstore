#[macro_use] extern crate log;
#[macro_use] extern crate serde_json;
extern crate simple_logger;

// my files/modules
mod message_file;

fn main() {
    simple_logger::init().unwrap();  // this will panic on error

    info!("Info");
    warn!("Warn");
    error!("Error");
}
