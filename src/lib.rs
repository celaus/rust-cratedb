#![feature(proc_macro)]

extern crate hyper;
extern crate erased_serde;
extern crate serde_json;

mod error;
pub mod row;
mod rowiterator;
mod backend;

use row::Row;
use self::serde_json::Value;
use self::serde_json::Map as JsonMap;
use self::hyper::Url;
use error::{CrateDBError, CrateDBConfigurationError};
use self::erased_serde::{Serialize, Serializer};
use rowiterator::RowIterator;
use std::collections::HashMap;
use std::error::Error;

use backend::{Backend, HTTPBackend};


pub type Cluster = DBCluster<HTTPBackend>;

pub struct DBCluster<T: Backend + Sized> {
    pub nodes: Vec<Url>,
    pub backend: T,
    node_rr: usize,
}


impl<T: Backend + Sized> DBCluster<T> {
    fn choose_node_endpoint(&mut self) -> Option<String> {
        if (self.nodes.len() > 0) {
            self.node_rr += 1;
            self.node_rr = self.node_rr % self.nodes.len();
            let host = self.nodes.get(self.node_rr).unwrap().as_str();
            return Some(format!("{}{}", host, "_sql".to_owned()));
        } else {
            return None;
        }
    }

    pub fn new(nodes: Vec<Url>) -> Result<Cluster, CrateDBConfigurationError> {
        if (nodes.len() < 1) {
            Err(CrateDBConfigurationError {
                description: "Please provide URLs to connect to".to_owned(),
            })
        } else {
            Ok(DBCluster {
                nodes: nodes,
                backend: HTTPBackend::new(),
                node_rr: 0,
            })
        }

    }

    pub fn with_custom_backend(nodes: Vec<Url>, backend: T) -> DBCluster<T> {
        DBCluster {
            nodes: nodes,
            backend: backend,
            node_rr: 0,
        }
    }

    /// Creates a cluster from a series of comma-separated urls (addess:port pairs)
    ///
    pub fn from_string(node_str: String) -> Result<Cluster, CrateDBConfigurationError> {
        let backend = HTTPBackend::new();
        let nodes: Vec<Url> = node_str.split(",").map(|n| Url::parse(n).unwrap()).collect();
        if (nodes.len() < 1) {
            Err(CrateDBConfigurationError {
                description: "Please provide URLs to connect to".to_owned(),
            })
        } else {
            Ok(DBCluster::with_custom_backend(nodes, backend))
        }
    }

    fn build_bulk_payload(&self, sql: &str, params: Box<Serialize>) -> String {
        let mut map = JsonMap::new();
        map.insert("stmt".to_string(), Value::String(sql.to_owned()));
        map.insert("bulk_args".to_string(), serde_json::to_value(params));
        return serde_json::to_string(&map).unwrap();

    }

    fn build_payload(&self, sql: &str, params: Option<Box<Serialize>>) -> String {
        let mut map = JsonMap::new();
        map.insert("stmt".to_string(), Value::String(sql.to_owned()));
        if let Some(p) = params {
            map.insert("args".to_string(), serde_json::to_value(p));
        }
        return serde_json::to_string(&map).unwrap();
    }

    fn crate_error(&self, payload: &Value) -> CrateDBError {
        let message = payload.pointer("/error/message").unwrap().as_str().unwrap();
        let code = payload.pointer("/error/code").unwrap().as_i64().unwrap();
        return CrateDBError::new(message.to_owned(), format!("{}", code));
    }


    /// Runs a query. Returns the results and the duration
    pub fn query(&mut self,
                 sql: &str,
                 params: Option<Box<Serialize>>)
                 -> Result<(f64, RowIterator), CrateDBError> {
        let url = self.choose_node_endpoint();
        let json_query = self.build_payload(sql, params);
        let body = try!(self.backend
            .execute(url, json_query)
            .map_err(|e| CrateDBError::new(e.description().to_owned(), "404".to_string())));
        if let Ok(raw) = serde_json::from_str(&body) {
            let data: Value = raw;
            println!("response: {}", body);

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
        return Err(CrateDBError::new("Invalid JSON was returned".to_owned(), "500".to_owned()));
    }

    /// Runs a query. Returns the results and the duration
    pub fn bulk_query(&mut self,
                      sql: &str,
                      params: Box<Serialize>)
                      -> Result<(f64, Vec<i64>), CrateDBError> {
        let url = self.choose_node_endpoint();
        let json_query = self.build_bulk_payload(sql, params);
        let body = try!(self.backend
            .execute(url, json_query)
            .map_err(|e| CrateDBError::new(e.description().to_owned(), "404".to_string())));

        if let Ok(raw) = serde_json::from_str(&body) {
            let data: Value = raw;
            println!("response: {}", body);

            return match data.pointer("/cols") {
                Some(cols_raw) => {
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
        return Err(CrateDBError::new("Invalid JSON was returned".to_owned(), "500".to_owned()));
    }
}

#[cfg(test)]
mod tests {

    use super::Backend;
    use super::error::{BackendError, CrateDBError, CrateDBConfigurationError};
    use std::collections::HashMap;
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
            if (!self.failing) {
                return Ok(self.response.clone());
            } else {
                return Err(BackendError { description: self.response.clone() });
            }
        }
    }


    fn new_cluster(response: &str, failing: bool) -> DBCluster<MockBackend> {
        DBCluster::with_custom_backend(vec![], MockBackend::new(response.to_owned(), failing))
    }

    #[test]
    fn parameter_query() {
        let mut cluster = new_cluster("{\"cols\":[\"name\"],\"rows\":[[\"A\"]],\"rowcount\":1,\
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
        let mut cluster = new_cluster("{\"cols\":[\"name\"],\"rows\":[[\"A\"]],\"rowcount\":1,\
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
        let mut cluster = new_cluster("{\"cols\": [], \"results\":[{\"rowcount\": 1}, \
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
        let mut cluster = new_cluster("{\"cols\":[\"name\"],\"rows\":[[\"A\"]],\"rowcount\":1,\
                                       \"duration\":0.206}",
                                      true);
        let result = cluster.bulk_query("select name from mytable where a = ?",
                                        Box::new(vec!["hello", "world", "lalala"]));
        assert!(result.is_err());
    }

    #[test]
    fn error_parameter_query() {
        let mut cluster = new_cluster("{\"error\":{\"message\":\"ReadOnlyException[Only read operations are allowed on this node]\",\"code\":5000}}",
                                      true);
        let result = cluster.query("create table a(a string, b long)", None);
        assert!(result.is_err());
        let e = result.err().unwrap();
        let expected = CrateDBError::new("ReadOnlyException[Only read operations are allowed on this node]".to_string(), "5000".to_string());
        assert_eq!(e, expected);

    }
}
