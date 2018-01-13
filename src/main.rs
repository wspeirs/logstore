extern crate pretty_env_logger;
#[macro_use] extern crate log;

// my files/modules
mod message_file;

fn main() {
    pretty_env_logger::init().unwrap();  // this will panic on error

    info!("Info");
    warn!("Warn");
    error!("Error");
}
