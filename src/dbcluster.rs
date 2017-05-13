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
extern crate rand;
extern crate serde;
extern crate hyper;
extern crate serde_json;


use Cluster;
use self::hyper::Url;
use error::CrateDBConfigurationError;
use std::convert::Into;
use self::rand::random;
use backend::{Backend, DefaultHTTPBackend};

///
/// Empty struct to pass into argument lists for the Box to have a type.
///
#[derive(Serialize)]
pub struct Nothing {}


///
/// Endpoint types to distinguish between URLs (/_sql vs /_blobs).
///
pub enum EndpointType {
    SQL,
    Blob,
}

/// Shortcut to access a CrateDB cluster with the default HTTP-based backend.

///
/// A CrateDB cluster
///
pub struct DBCluster<T: Backend + Sized> {
    /// A collection of URLs to the available nodes
    pub nodes: Vec<Url>,

    /// The backend with which the nodes/URLs can be reached
    pub backend: T,
}


///
/// Trait to expose load balancing features of the driver to
/// other components. Should return a URL for the backend to use.
///
pub trait Loadbalancing {
    ///
    /// Returns an endpoint for the provided URL type (BLOB or SQL).
    ///
    fn get_endpoint(&self, endpoint_type: EndpointType) -> Option<String>;
}

impl<T: Backend + Sized> Loadbalancing for DBCluster<T> {
    // Chooses a new node using a random strategy
    fn get_endpoint(&self, endpoint_type: EndpointType) -> Option<String> {
        if !self.nodes.is_empty() {
            let node = random::<usize>() % self.nodes.len();
            let host = self.nodes[node].as_str();
            let t = match endpoint_type {
                EndpointType::SQL => "_sql",
                EndpointType::Blob => "_blobs",
            };
            Some(format!("{}{}", host, t))
        } else {
            None
        }
    }
}


impl<T: Backend + Sized> DBCluster<T> {
    ///
    /// Creates a new HTTP-backed cluster object with the provided URLs.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use cratedb::Cluster;
    /// use hyper::Url;
    /// let mut c: Cluster = Cluster::new(vec![Url::parse("http://localhost:4200")]);
    /// ```
    pub fn new(nodes: Vec<Url>) -> Result<DBCluster<DefaultHTTPBackend>, CrateDBConfigurationError> {
        if nodes.len() < 1 {
            Err(CrateDBConfigurationError {
                description: String::from("Please provide URLs to connect to"),
            })
        } else {
            Ok(DBCluster {
                nodes: nodes,
                backend: DefaultHTTPBackend::new(),
            })
        }

    }

    ///
    /// Creates a new HTTP-backed cluster object with the provided URLs.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use cratedb::Cluster;
    /// use hyper::Url;
    /// let mut c: Cluster = Cluster::with_proxy(vec![Url::parse("http://localhost:4200")], "localhost", 12345);
    /// ```
    pub fn with_proxy(nodes: Vec<Url>,
                      host: &'static str,
                      port: u16)
                      -> Result<Cluster, CrateDBConfigurationError> {
        if nodes.len() < 1 {
            Err(CrateDBConfigurationError {
                description: String::from("Please provide URLs to connect to"),
            })
        } else {
            Ok(DBCluster {
                nodes: nodes,
                backend: DefaultHTTPBackend::with_proxy(host, port),
            })
        }

    }

    ///
    /// Creates a new HTTP-backed cluster object with the provided URLs and
    /// a custom backend.
    ///
    pub fn with_custom_backend(nodes: Vec<Url>, backend: T) -> DBCluster<T> {
        DBCluster {
            nodes: nodes,
            backend: backend,
        }
    }

    ///
    /// Creates a cluster from a series of comma-separated urls (addess:port pairs)
    ///
    /// # Example
    ///
    /// ```rust
    /// use cratedb::Cluster;
    /// let node1 = "http://localhost:4200/";
    /// let node2 = "http://play.crate.io/";
    /// let mut c: Cluster = Cluster::from_string(format!("{},{}", node1, node2)).unwrap();
    /// assert_eq!(c.nodes.get(0).unwrap().to_string(), node1.to_string());
    /// assert_eq!(c.nodes.get(1).unwrap().to_string(), node2.to_string());
    /// ```
    pub fn from_string<S>(node_str: S) -> Result<Cluster, CrateDBConfigurationError>
        where S: Into<String>
    {
        let backend = DefaultHTTPBackend::new();
        let nodes: Vec<Url> = node_str.into().split(',').map(|n| Url::parse(n).unwrap()).collect();
        if nodes.len() < 1 {
            Err(CrateDBConfigurationError {
                description: String::from("Please provide URLs to connect to"),
            })
        } else {
            Ok(DBCluster::with_custom_backend(nodes, backend))
        }
    }
}