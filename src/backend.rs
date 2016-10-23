extern crate hyper;
use self::hyper::Client;

use std::io::Read;
use std::error::Error;
use error::BackendError;


pub struct HTTPBackend {}

pub trait Backend {
    fn execute(&self, to: &String, payload: String) -> Result<String, BackendError>;
}

impl HTTPBackend {
    pub fn new() -> HTTPBackend {
        HTTPBackend {}
    }
}

impl Backend for HTTPBackend {
    fn execute(&self, to: &String, payload: String) -> Result<String, BackendError> {
        let client = Client::new();
        let mut response = try!(client.post(to)
            .body(&payload)
            .send()
            .map_err(|e| BackendError { description: e.description().to_owned() }));
        let mut buf = String::new();
        try!(response.read_to_string(&mut buf)
            .map_err(|e| BackendError { description: e.description().to_owned() }));
        println!("result: {}", &buf);
        return Ok(buf);
    }
}
