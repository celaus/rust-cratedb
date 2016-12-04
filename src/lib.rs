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
use error::{StringError, CrateError};
use self::erased_serde::{Serialize, Serializer};
use rowiterator::RowIterator;
use std::collections::HashMap;
use std::error::Error;

use backend::{Backend, HTTPBackend};

pub struct Cluster {
    pub nodes: Vec<Url>,
    pub backend: HTTPBackend,
    pub last_duration: u64,
}


impl Cluster {
    fn choose_node_endpoint(&self) -> Option<String> {
        let host = self.nodes.get(0).unwrap().as_str();
        return Some(format!("{}{}", host, "_sql".to_owned()));
    }

    pub fn new(nodes: Vec<Url>) -> Cluster {
        Cluster {
            nodes: nodes,
            backend: HTTPBackend::new(),
            last_duration: 0,
        }
    }

    pub fn with_custom_backend(nodes: Vec<Url>, backend: HTTPBackend) -> Cluster {
        Cluster {
            nodes: nodes,
            backend: backend,
            last_duration: 0,
        }
    }

    /// Creates a cluster from a series of comma-separated urls (addess:port pairs)
    ///
    pub fn from_string(node_str: String) -> Result<Cluster, StringError> {
        let backend = HTTPBackend::new();
        Ok(Cluster::with_custom_backend(node_str.split(",")
                                            .map(|n| Url::parse(n).unwrap())
                                            .collect(),
                                        backend))
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

    fn crate_error(&self, payload: &Value) -> CrateError {
        let message = payload.pointer("/error/message").unwrap().as_str().unwrap();
        let code = payload.pointer("/error/code").unwrap().as_i64().unwrap();
        return CrateError::new(message.to_owned(), format!("{}", code));
    }


    /// Runs a query. Returns the results and the duration
    pub fn query(&mut self,
                 sql: &str,
                 params: Option<Box<Serialize>>)
                 -> Result<(f64, RowIterator), CrateError> {
        let url = self.choose_node_endpoint().unwrap();
        let json_query = self.build_payload(sql, params);
        let body = try!(self.backend
            .execute(&url, json_query)
            .map_err(|e| CrateError::new(e.description().to_owned(), "404".to_string())));
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
                    None => Err(self.crate_error(&data))

                };
            }
            return Err(CrateError::new("sth went wrong".to_owned(), "500".to_owned()));    }

    /// Runs a query. Returns the results and the duration
    pub fn bulk_query(&mut self,
                      sql: &str,
                      params: Box<Serialize>)
                      -> Result<(f64, Vec<i64>), CrateError> {
        let url = self.choose_node_endpoint().unwrap();
        let json_query = self.build_bulk_payload(sql, params);
        let body = try!(self.backend
            .execute(&url, json_query)
            .map_err(|e| CrateError::new(e.description().to_owned(), "404".to_string())));

        if let Ok(raw) = serde_json::from_str(&body) {
            let data: Value = raw;
            println!("response: {}", body);

            return match data.pointer("/cols") {
                Some(cols_raw) => {
                    let bulk_results = data.pointer("/results").unwrap().as_array().unwrap();
                    let rowcounts =  bulk_results.into_iter()
                        .map(|v| v.pointer("/rowcount").unwrap().as_i64().unwrap())
                        .collect();
                    let duration = data.pointer("/duration").unwrap().as_f64().unwrap();
                    Ok((duration, rowcounts))
                }
                None => Err(self.crate_error(&data))
            };
        }
        return Err(CrateError::new("sth went wrong".to_owned(), "500".to_owned()));
    }
}
