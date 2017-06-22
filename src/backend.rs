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

impl Into<UrlType> for String {
    fn into(self) -> UrlType {
        match self.as_ref() {
            "https" => UrlType::Encryped,
            _ => UrlType::Plaintext,
        }
    }
}

pub type DefaultHTTPBackend = HTTPBackend<&'static str>;

pub enum BackendResult {
    NotFound,
    NotAuthorized,
    Timeout,
    Error,
    Ok,
}

pub trait Backend {
    ///
    /// Executes a SQL command
    ///
    fn execute(&self, to: Option<String>, payload: String) -> Result<String, BackendError>;

    ///
    /// Uploads a blob to the given URL, bucket, sha1.
    ///
    /// # Errors
    /// For invalid URLs, errors while reading the Read object, or connection errors (depends on the implementation).
    ///
    /// # Examples
    /// ```rust,ignore
    /// backend.upload_blob(Some(url), "my_bucket", &blob.sha1, &mut my_file)
    /// ```
    ///
    fn upload_blob(&self,
                   to: Option<String>,
                   bucket: &str,
                   sha1: &[u8],
                   f: &mut Read)
                   -> Result<BackendResult, BackendError>;

    ///
    /// Deletes a blob.
    ///
    /// # Errors
    /// For invalid URLs or connection errors (depends on the implementation).
    ///
    /// # Examples
    /// ```rust,ignore
    /// backend.delete_blob(Some(url), "my_bucket", &blob.sha1)
    /// ```
    ///
    fn delete_blob(&self,
                   to: Option<String>,
                   bucket: &str,
                   sha1: &[u8])
                   -> Result<BackendResult, BackendError>;

    ///
    /// Retrieves a blob from the given URL, bucket, sha1.
    ///
    /// # Panics
    /// # Errors
    /// For invalid URLs or connection errors (depends on the implementation).
    ///
    /// # Examples
    /// ```rust,ignore
    /// backend.fetch_blob(Some(url), "my_bucket", &blob.sha1)
    /// ```
    ///
    fn fetch_blob(&self,
                  to: Option<String>,
                  bucket: &str,
                  sha1: &[u8])
                  -> Result<(BackendResult, Box<Read>), BackendError>;
}

pub struct HTTPBackend<H: Into<Cow<'static, str>> + Clone> {
    client_factory: HTTPClientFactory<H>,
}


impl<H: Into<Cow<'static, str>> + Clone> HTTPBackend<H> {
    pub fn new() -> DefaultHTTPBackend {
        HTTPBackend { client_factory: HTTPClientFactory::<H>::new() }
    }

    pub fn with_proxy(host: H, port: u16) -> HTTPBackend<H> {
        HTTPBackend { client_factory: HTTPClientFactory::with_proxy(host, port) }
    }
}


impl<H: Into<Cow<'static, str>> + Clone> Backend for HTTPBackend<H> {
    fn execute(&self, to: Option<String>, payload: String) -> Result<String, BackendError> {

        let to_raw = to.ok_or(BackendError::new("No URL specified".to_owned()))?;
        let to = Url::parse(&to_raw).unwrap();
        let client = self.client_factory
            .client(match to.scheme() {
                        "http" => UrlType::Plaintext,
                        "https" => UrlType::Encryped,
                        _ => return Err(BackendError::new("Unknown URL scheme".to_string())),
                    });

        let mut response = try!(client
                                    .post(to)
                                    .body(&payload)
                                    .send()
                                    .map_err(|e| BackendError::from_transport(e)));

        let mut buf = String::new();
        response
            .read_to_string(&mut buf)
            .map_err(|e| BackendError::from_io(e))?;
        Ok(buf)
    }

    fn upload_blob(&self,
                   to: Option<String>,
                   bucket: &str,
                   sha1: &[u8],
                   mut f: &mut Read)
                   -> Result<BackendResult, BackendError> {
        if let Ok(to) = make_blob_url(to, bucket, sha1) {
            let client = self.client_factory.client(to.scheme().to_string());
            client
                .put(to)
                .body(Body::ChunkedBody(&mut f))
                .send()
                .map(|r| parse_status(&r.status))
                .map_err(|e| BackendError::from_transport(e))
        } else {
            Err(BackendError::new("Invalid blob url".to_string()))
        }
    }


    fn delete_blob(&self,
                   to: Option<String>,
                   bucket: &str,
                   sha1: &[u8])
                   -> Result<BackendResult, BackendError> {
        if let Ok(to) = make_blob_url(to, bucket, sha1) {
            let client = self.client_factory.client(to.scheme().to_string());
            client
                .delete(to)
                .send()
                .map(|r| parse_status(&r.status))
                .map_err(|e| BackendError::from_transport(e))
        } else {
            Err(BackendError::new("Invalid blob url".to_string()))
        }
    }

    fn fetch_blob(&self,
                  to: Option<String>,
                  bucket: &str,
                  sha1: &[u8])
                  -> Result<(BackendResult, Box<Read>), BackendError> {

        if let Ok(to) = make_blob_url(to, bucket, sha1) {
            let client = self.client_factory.client(to.scheme().to_string());

            let response = client
                .get(to)
                .send()
                .map_err(|e| BackendError::from_transport(e))?;
            Ok((parse_status(&response.status), Box::new(response)))
        } else {
            Err(BackendError::new("Invalid blob url".to_string()))
        }
    }
}


fn parse_status(code: &StatusCode) -> BackendResult {
    match *code {
        StatusCode::Ok | StatusCode::Created | StatusCode::Accepted => BackendResult::Ok, 
        StatusCode::BadRequest |
        StatusCode::InternalServerError => BackendResult::Error,
        StatusCode::Unauthorized |
        StatusCode::Forbidden |
        StatusCode::MethodNotAllowed => BackendResult::NotAuthorized,
        StatusCode::RequestTimeout => BackendResult::Timeout,
        _ => BackendResult::Error,
    }
}

fn make_blob_url(to: Option<String>, bucket: &str, sha1: &[u8]) -> Result<Url, BackendError> {
    let to_raw = to.ok_or(BackendError::new("No URL specified".to_owned()))?;
    if let Ok(to) = Url::parse(&to_raw) {
        let sha1_str = to_hex_string(sha1);
        let mut path = PathBuf::from(to.path());
        path.push(bucket);
        path.push(sha1_str);
        if let Some(url_remainder) = path.to_str() {
            to.join(url_remainder)
                .map_err(|e| BackendError::from_parser(e))
        } else {
            Err(BackendError::new("Invalid bytes in path".to_string()))
        }
    } else {
        Err(BackendError::new("Invalid URL".to_string()))
    }
}


///
/// Client factory for loosely coupling the backend's clients. Mainly for testability.
///
trait ClientFactory {
    fn client<T>(&self, u: T) -> Client where T: Into<UrlType>;
}

struct HTTPClientFactory<H: Into<Cow<'static, str>> + Clone> {
    use_proxy: bool,
    proxy_host: H,
    proxy_port: u16,
}

impl<H: Into<Cow<'static, str>> + Clone> HTTPClientFactory<H> {
    pub fn new() -> HTTPClientFactory<&'static str> {
        HTTPClientFactory {
            proxy_host: "",
            proxy_port: 0,
            use_proxy: false,
        }
    }

    pub fn with_proxy(host: H, port: u16) -> HTTPClientFactory<H> {
        HTTPClientFactory {
            proxy_host: host,
            proxy_port: port,
            use_proxy: true,
        }
    }
}

impl<H: Into<Cow<'static, str>> + Clone> ClientFactory for HTTPClientFactory<H> {
    fn client<T>(&self, u: T) -> Client
        where T: Into<UrlType>
    {
        if self.use_proxy {
            Client::with_http_proxy(self.proxy_host.clone(), self.proxy_port)
        } else {
            match u.into() {
                UrlType::Encryped => Client::with_connector(HttpConnector {}),
                UrlType::Plaintext => Client::with_connector(HttpsConnector::new(TlsClient::new())),
            }
        }
    }
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
        assert_eq!(make_blob_url(None, "a", b"1234"),
                   Err(BackendError::new("No URL specified".to_string())));

        assert_eq!(make_blob_url(Some("https://my_url".to_string()), "a", b"1234").ok(),
                   Some(Url::parse("https://my_url/a/31323334").unwrap()));

        assert_eq!(make_blob_url(Some("https://localhost:4200/_blobs".to_string()),
                                 "my_table",
                                 b"ff")
                           .ok(),
                   Some(Url::parse("https://localhost:4200/_blobs/my_table/6666").unwrap()));
    }
}
