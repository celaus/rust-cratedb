extern crate hyper;
use self::hyper::Client;

use std::io::Read;
use std::error::Error;
use error::BackendError;

pub trait Backend {
    fn execute(&self, to: Option<String>, payload: String) -> Result<String, BackendError>;
}

pub struct HTTPBackend {}


impl HTTPBackend {
    pub fn new() -> HTTPBackend {
        HTTPBackend {}
    }
}

impl Backend for HTTPBackend {
    fn execute(&self, to: Option<String>, payload: String) -> Result<String, BackendError> {
        println!("sending {}", payload);
        let to = try!(to.ok_or(BackendError { description: "No URL specified".to_owned() }));
        let client = Client::new();
        let mut response = try!(client.post(&to)
            .body(&payload)
            .send()
            .map_err(|e| BackendError { description: e.description().to_owned() }));
        let mut buf = String::new();
        try!(response.read_to_string(&mut buf)
            .map_err(|e| BackendError { description: e.description().to_owned() }));
        return Ok(buf);
    }
}
