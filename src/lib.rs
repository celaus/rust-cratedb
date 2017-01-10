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
extern crate erased_serde;
extern crate serde_json;
extern crate rand;


pub mod error;
pub mod row;

mod rowiterator;
mod backend;

use self::serde_json::Value;
use self::serde_json::Map as JsonMap;
use self::hyper::Url;
use error::{CrateDBError, CrateDBConfigurationError};
use self::erased_serde::Serialize;
use rowiterator::RowIterator;
use std::collections::HashMap;
use std::convert::Into;
use self::rand::random;

use backend::{Backend, DefaultHTTPBackend};


/// Shortcut to access a CrateDB cluster with the default HTTP-based backend.
pub type Cluster = DBCluster<DefaultHTTPBackend>;


///
/// A CrateDB cluster
///
pub struct DBCluster<T: Backend + Sized> {
    /// A collection of URLs to the available nodes
    pub nodes: Vec<Url>,

    /// The backend with which the nodes/URLs can be reached
    pub backend: T,
}


impl<T: Backend + Sized> DBCluster<T> {
    // Chooses a new node using a random strategy
    fn choose_node_endpoint(&self) -> Option<String> {
        if self.nodes.len() > 0 {
            let node = random::<usize>() % self.nodes.len();
            let host = self.nodes.get(node).unwrap().as_str();
            return Some(format!("{}{}", host, "_sql"));
        } else {
            return None;
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
    /// let mut c: Cluster = Cluster::new(vec![Url::parse("http://localhost:4200")]);
    /// ```
    pub fn new(nodes: Vec<Url>) -> Result<Cluster, CrateDBConfigurationError> {
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
    /// let mut c: Cluster = Cluster::new(vec![Url::parse("http://localhost:4200")]);
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
        let nodes: Vec<Url> = node_str.into().split(",").map(|n| Url::parse(n).unwrap()).collect();
        if nodes.len() < 1 {
            Err(CrateDBConfigurationError {
                description: String::from("Please provide URLs to connect to"),
            })
        } else {
            Ok(DBCluster::with_custom_backend(nodes, backend))
        }
    }

    // Executes the query against the backend.
    fn execute<S>(&self, sql: S, bulk: bool, params: Option<Box<Serialize>>) -> String
        where S: Into<String>
    {
        let url = self.choose_node_endpoint();
        let json_query = if bulk {
            self.build_bulk_payload(sql, params.unwrap_or(Box::new("{}")))
        } else {
            self.build_payload(sql, params)
        };
        return match self.backend.execute(url, json_query) {
            Ok(r) => r,
            Err(e) => e.response,
        };
    }

    fn build_bulk_payload<S>(&self, sql: S, params: Box<Serialize>) -> String
        where S: Into<String>
    {
        let mut map: JsonMap<&'static str, Value> = JsonMap::new();
        map.insert("stmt", Value::String(sql.into()));
        map.insert("bulk_args", serde_json::to_value(params));
        return serde_json::to_string(&map).unwrap();

    }

    fn build_payload<S>(&self, sql: S, params: Option<Box<Serialize>>) -> String
        where S: Into<String>
    {
        let mut map: JsonMap<&'static str, Value> = JsonMap::new();
        map.insert("stmt", Value::String(sql.into()));
        if let Some(p) = params {
            map.insert("args", serde_json::to_value(p));
        }
        return serde_json::to_string(&map).unwrap();
    }

    fn crate_error(&self, payload: &Value) -> CrateDBError {
        let message = payload.pointer("/error/message").unwrap().as_str().unwrap();
        let code = payload.pointer("/error/code").unwrap().as_i64().unwrap();
        return CrateDBError::new(message, format!("{}", code));
    }

    fn invalid_json(&self, body: String) -> CrateDBError {
        CrateDBError::new(format!("{}: {}", "Invalid JSON was returned", body), "500")
    }

    ///
    /// Runs a query. Returns the results and the duration
    ///
    /// # Example
    ///
    /// ```
    /// use cratedb::Cluster;
    /// use cratedb::row::ByIndex;
    /// let node = "http://play.crate.io";
    /// let mut c: Cluster = Cluster::from_string(node).unwrap();
    /// let (elapsed, rows) = c.query("select hostname from sys.nodes", None).unwrap();
    ///
    /// for r in rows {
    ///  println!("{}", r.as_string(0).unwrap());
    /// }
    /// ```
    pub fn query<S>(&self,
                    sql: S,
                    params: Option<Box<Serialize>>)
                    -> Result<(f64, RowIterator), CrateDBError>
        where S: Into<String>
    {

        let body = self.execute(sql, false, params);
        if let Ok(raw) = serde_json::from_str(&body) {

            let data: Value = raw;
            return match data.pointer("/cols") {
                Some(cols_raw) => {
                    let rows = data.pointer("/rows").unwrap().as_array().unwrap();
                    let cols_raw = cols_raw.as_array().unwrap();
                    let mut cols = HashMap::with_capacity(cols_raw.len());
                    for (i, c) in cols_raw.iter().enumerate() {
                        let _ = match *c {
                            Value::String(ref name) => cols.insert(name.to_owned(), i),
                            _ => None,
                        };
                    }

                    let duration = data.pointer("/duration").unwrap().as_f64().unwrap();
                    Ok((duration, RowIterator::new(rows.clone(), cols)))
                }
                None => Err(self.crate_error(&data)),

            };
        }
        return Err(self.invalid_json(body));
    }


    /// Runs a query. Returns the results and the duration
    /// ```
    /// use doc::Cluster;
    /// use doc::row::ByIndex;
    /// let node = "http://play.crate.io";
    /// let mut c: Cluster = Cluster::from_string(node).unwrap();
    /// let (elapsed, rows) = c.query("select hostname from sys.nodes", None).unwrap();
    ///
    /// for r in rows {
    ///  println!(r.as_string(0).unwrap());
    /// }
    /// ```
    pub fn bulk_query<S>(&self,
                         sql: S,
                         params: Box<Serialize>)
                         -> Result<(f64, Vec<i64>), CrateDBError>
        where S: Into<String>
    {

        let body = self.execute(sql, true, Some(params));

        if let Ok(raw) = serde_json::from_str(&body) {
            let data: Value = raw;

            return match data.pointer("/cols") {
                Some(_) => {
                    let bulk_results = data.pointer("/results").unwrap().as_array().unwrap();
                    let rowcounts = bulk_results.into_iter()
                        .map(|v| v.pointer("/rowcount").unwrap().as_i64().unwrap())
                        .collect();
                    let duration = data.pointer("/duration").unwrap().as_f64().unwrap();
                    Ok((duration, rowcounts))
                }
                None => Err(self.crate_error(&data)),
            };
        }
        return Err(self.invalid_json(body));
    }
}

#[cfg(test)]
mod tests {

    use super::Backend;
    use super::error::{BackendError, CrateDBError};
    use super::DBCluster;
    use super::row::{Row, ByIndex};

    struct MockBackend {
        failing: bool,
        response: String,
    }


    impl MockBackend {
        pub fn new(response: String, failing: bool) -> MockBackend {
            MockBackend {
                failing: failing,
                response: response,
            }
        }
    }

    impl Backend for MockBackend {
        fn execute(&self, to: Option<String>, payload: String) -> Result<String, BackendError> {
            let _ = (to, payload);
            if !self.failing {
                return Ok(self.response.clone());
            } else {
                return Err(BackendError { response: self.response.clone() });
            }
        }
    }


    fn new_cluster(response: &str, failing: bool) -> DBCluster<MockBackend> {
        DBCluster::with_custom_backend(vec![], MockBackend::new(response.to_owned(), failing))
    }

    #[test]
    fn parameter_query() {
        let cluster = new_cluster("{\"cols\":[\"name\"],\"rows\":[[\"A\"]],\"rowcount\":1,\
                                       \"duration\":0.206}",
                                  false);
        let result = cluster.query("select name from mytable where a = ?",
                                   Some(Box::new("hello")));
        assert!(result.is_ok());
        let (t, result) = result.unwrap();
        assert_eq!(t, 0.206f64);
        let rows: Vec<Row> = result.collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.get(0).unwrap().as_string(0).unwrap(), "A".to_owned());
    }

    #[test]
    fn no_parameter_query() {
        let cluster = new_cluster("{\"cols\":[\"name\"],\"rows\":[[\"A\"]],\"rowcount\":1,\
                                       \"duration\":0.206}",
                                  false);
        let result = cluster.query("select name from mytable where a = 'hello'", None);
        assert!(result.is_ok());
        let (t, result) = result.unwrap();
        assert_eq!(t, 0.206f64);
        let rows: Vec<Row> = result.collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.get(0).unwrap().as_string(0).unwrap(), "A".to_owned());
    }

    #[test]
    fn bulk_parameter_query() {
        let cluster = new_cluster("{\"cols\": [], \"results\":[{\"rowcount\": 1}, \
                                       {\"rowcount\": 2}, {\"rowcount\": 3}],
                                       \
                                       \"duration\":0.206}",
                                  false);
        let result = cluster.bulk_query("update mytable set v = 1 where a = ?",
                                        Box::new(vec!["hello", "world", "lalala"]));
        assert!(result.is_ok());
        let (t, result) = result.unwrap();
        assert_eq!(t, 0.206f64);
        assert_eq!(result.len(), 3);
        assert_eq!(result.get(0).unwrap(), &1i64);
        assert_eq!(result.get(1).unwrap(), &2i64);
        assert_eq!(result.get(2).unwrap(), &3i64);
    }

    #[test]
    fn error_bulk_parameter_query() {
        let cluster = new_cluster("{\"error\":{\"message\":\"ReadOnlyException[Only read \
                                       operations are allowed on this node]\",\"code\":5000}}",
                                  true);
        let result = cluster.bulk_query("select name from mytable where a = ?",
                                        Box::new(vec!["hello", "world", "lalala"]));
        assert!(result.is_err());
        let e = result.err().unwrap();
        let expected = CrateDBError::new("ReadOnlyException[Only read operations are allowed on \
                                          this node]",
                                         "5000");
        assert_eq!(e, expected);

    }

    #[test]
    fn error_parameter_query() {
        let cluster = new_cluster("{\"error\":{\"message\":\"ReadOnlyException[Only read \
                                       operations are allowed on this node]\",\"code\":5000}}",
                                  true);
        let result = cluster.query("create table a(a string, b long)", None);
        assert!(result.is_err());
        let e = result.err().unwrap();
        let expected = CrateDBError::new("ReadOnlyException[Only read operations are allowed on \
                                          this node]",
                                         "5000");
        assert_eq!(e, expected);
    }

    #[test]
    fn non_json_backend_error() {
        let cluster = new_cluster("this is wrong my friend :{", true);


        let result = cluster.query("select * from sys.nodes", None);
        assert!(result.is_err());
        let e = result.err().unwrap();
        let expected = CrateDBError::new("Invalid JSON was returned: this is wrong my friend :{",
                                         "500");
        assert_eq!(e, expected);

        // bulk queries:
        let result = cluster.bulk_query("select * from sys.nodes", Box::new("{}"));
        assert!(result.is_err());
        let e = result.err().unwrap();
        let expected = CrateDBError::new("Invalid JSON was returned: this is wrong my friend :{",
                                         "500");
        assert_eq!(e, expected);

    }
}
