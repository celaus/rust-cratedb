// Copyright 2016 Claus Matzinger
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate hyper;
use self::hyper::Client;

use std::io::Read;
use std::error::Error;
use error::BackendError;
use std::borrow::Cow;
use std::convert::Into;
use std::clone::Clone;


pub type DefaultHTTPBackend = HTTPBackend<&'static str>;

pub trait Backend {
    fn execute(&self, to: Option<String>, payload: String) -> Result<String, BackendError>;
}

pub struct HTTPBackend<H: Into<Cow<'static, str>> + Clone> {
    proxy_host: H,
    proxy_port: u16,
    use_proxy: bool,
}


impl<H: Into<Cow<'static, str>> + Clone> HTTPBackend<H> {
    pub fn new() -> DefaultHTTPBackend {
        HTTPBackend {
            proxy_host: "",
            proxy_port: 0,
            use_proxy: false,
        }
    }

    pub fn with_proxy(host: H, port: u16) -> HTTPBackend<H> {
        HTTPBackend {
            proxy_host: host,
            proxy_port: port,
            use_proxy: true,
        }
    }
}

impl<H: Into<Cow<'static, str>> + Clone> Backend for HTTPBackend<H> {
    fn execute(&self, to: Option<String>, payload: String) -> Result<String, BackendError> {
        let to = try!(to.ok_or(BackendError { response: "No URL specified".to_owned() }));

        let client = if self.use_proxy {
            Client::with_http_proxy(self.proxy_host.clone(), self.proxy_port)
        } else {
            Client::new()
        };

        let mut response = try!(client.post(&to)
            .body(&payload)
            .send()
            .map_err(|e| BackendError { response: e.description().to_owned() }));

        let mut buf = String::new();
        try!(response.read_to_string(&mut buf)
            .map_err(|e| BackendError { response: e.description().to_owned() }));
        return Ok(buf);
    }
}
