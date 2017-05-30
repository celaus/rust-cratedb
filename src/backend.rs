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
extern crate hyper_rustls;

use self::hyper::{Client, Url};
use self::hyper::net::{HttpConnector, HttpsConnector};
use self::hyper::error::Error as HyperError;
use self::hyper_rustls::TlsClient;
use self::hyper::client::Body;
use self::hyper::client::response::Response;
use self::hyper::status::StatusCode;

use std::io::Read;
use std::error::Error;
use error::BackendError;
use std::borrow::Cow;
use std::convert::Into;
use std::clone::Clone;
use common::to_hex_string;
use std::path::PathBuf;

enum UrlType {
    Encryped,
    Plaintext,
}

pub type DefaultHTTPBackend = HTTPBackend<&'static str>;

pub trait Backend {
    fn execute(&self, to: Option<String>, payload: String) -> Result<String, BackendError>;
    fn upload_blob(&self,
                   to: Option<String>,
                   bucket: &str,
                   sha1: &[u8],
                   f: &mut Read)
                   -> Result<(), BackendError>;

    fn delete_blob(&self,
                   to: Option<String>,
                   bucket: &str,
                   sha1: &[u8])
                   -> Result<(), BackendError>;
    fn fetch_blob(&self,
                  to: Option<String>,
                  bucket: &str,
                  sha1: &[u8])
                  -> Result<Box<Read>, BackendError>;
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

    fn get_client(&self, u: UrlType) -> Client {
        if self.use_proxy {
            Client::with_http_proxy(self.proxy_host.clone(), self.proxy_port)
        } else {
            match u {
                UrlType::Encryped => Client::with_connector(HttpConnector {}),
                UrlType::Plaintext => Client::with_connector(HttpsConnector::new(TlsClient::new())),
            }
        }
    }

    fn parse_response(&self, response: Result<Response, HyperError>) -> Result<(), BackendError> {
        if let Ok(result) = response {
            match result.status {
                StatusCode::Created => Ok(()),
                _ => {
                    Err(BackendError::Custom {
                            message: format!("Returned HTTP status: {}", result.status),
                        })
                }

            }
        } else {
            Err(BackendError::Transport(response.unwrap_err()))
        }
    }
}

impl<H: Into<Cow<'static, str>> + Clone> Backend for HTTPBackend<H> {
    fn execute(&self, to: Option<String>, payload: String) -> Result<String, BackendError> {

        let to_raw = to.ok_or(BackendError::Custom { message: "No URL specified".to_owned() })?;
        let to = Url::parse(&to_raw).unwrap();
        let client = self.get_client(match to.scheme() {
                                         "http" => UrlType::Plaintext,
                                         "https" => UrlType::Encryped,
                                         _ => {
                                             return Err(BackendError::Custom {
                                                            message: "Unknown URL scheme"
                                                                .to_string(),
                                                        })
                                         }
                                     });

        let mut response = try!(client
                                    .post(to)
                                    .body(&payload)
                                    .send()
                                    .map_err(BackendError::Transport));

        let mut buf = String::new();
        response
            .read_to_string(&mut buf)
            .map_err(|e| BackendError::Io(e))?;
        Ok(buf)
    }

    fn upload_blob(&self,
                   to: Option<String>,
                   bucket: &str,
                   sha1: &[u8],
                   mut f: &mut Read)
                   -> Result<(), BackendError> {
        let to = make_blob_url(to, bucket, sha1).expect("Invalid blob url");
        let client = self.get_client(match to.scheme() {
                                         "http" => UrlType::Plaintext,
                                         "https" => UrlType::Encryped,
                                         _ => {
                                             return Err(BackendError::Custom {
                                                            message: "Unknown URL scheme"
                                                                .to_string(),
                                                        })
                                         }
                                     });
        let response = client.put(to).body(Body::ChunkedBody(&mut f)).send();
        self.parse_response(response)
    }


    fn delete_blob(&self,
                   to: Option<String>,
                   bucket: &str,
                   sha1: &[u8])
                   -> Result<(), BackendError> {
        let to = make_blob_url(to, bucket, sha1).expect("Invalid blob url");
        let client = self.get_client(match to.scheme() {
                                         "http" => UrlType::Plaintext,
                                         "https" => UrlType::Encryped,
                                         _ => {
                                             return Err(BackendError::Custom {
                                                            message: "Unknown URL scheme"
                                                                .to_string(),
                                                        })
                                         }
                                     });

        let _ = try!(client.delete(to).send().map_err(BackendError::Transport));

        Ok(())
    }

    fn fetch_blob(&self,
                  to: Option<String>,
                  bucket: &str,
                  sha1: &[u8])
                  -> Result<Box<Read>, BackendError> {

        let to = make_blob_url(to, bucket, sha1).expect("Invalid blob url");
        let client = self.get_client(match to.scheme() {
                                         "http" => UrlType::Plaintext,
                                         "https" => UrlType::Encryped,
                                         _ => {
                                             return Err(BackendError::Custom {
                                                            message: "Unknown URL scheme"
                                                                .to_string(),
                                                        })
                                         }
                                     });

        let response = try!(client.get(to).send().map_err(BackendError::Transport));

        Ok(Box::new(response))
    }
}

fn make_blob_url(to: Option<String>, bucket: &str, sha1: &[u8]) -> Result<Url, BackendError> {
    let to_raw = to.ok_or(BackendError::Custom { message: "No URL specified".to_owned() })?;
    let to = Url::parse(&to_raw).expect("Invalid URL");
    let sha1_str = to_hex_string(sha1);
    let mut path = PathBuf::from(to.path());
    path.push(bucket);
    path.push(sha1_str);
    Ok(to.join(path.to_str().expect("Invalid bytes in path"))
           .expect("Could not create URL"))
}


#[cfg(test)]
mod tests {
    use error::BackendError;
    use super::*;
    use super::make_blob_url;
    #[test]
    fn valid_make_blob_url() {
        assert_eq!(make_blob_url(Some("https://my_url".to_string()), "a", b"1234").ok(),
                   Some(Url::parse("https://my_url/a/31323334").unwrap()));

        assert_eq!(make_blob_url(Some("https://localhost:4200/_blobs".to_string()),
                                 "my_table",
                                 b"ff")
                           .ok(),
                   Some(Url::parse("https://localhost:4200/_blobs/my_table/6666").unwrap()));

    }

    #[test]
    fn invalid_make_blob_url() {
        // assert_eq!(make_blob_url(None, "a", b"1234"),
        //            Err(BackendError::Custom { message: "No URL specified".to_string() }));

        assert_eq!(make_blob_url(Some("https://my_url".to_string()), "a", b"1234").ok(),
                   Some(Url::parse("https://my_url/a/31323334").unwrap()));

        assert_eq!(make_blob_url(Some("https://localhost:4200/_blobs".to_string()),
                                 "my_table",
                                 b"ff")
                           .ok(),
                   Some(Url::parse("https://localhost:4200/_blobs/my_table/6666").unwrap()));
    }
}
