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
extern crate serde;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate serde_derive;


pub mod error;
pub mod row;
pub mod blob;
pub mod dbcluster;
pub mod sql;
mod rowiterator;
mod backend;

// use self::serde_json::Value;
// use self::serde::ser::Serialize;
// use self::hyper::Url;
// use error::{CrateDBError, CrateDBConfigurationError};
// use rowiterator::RowIterator;
// use std::collections::HashMap;
// use std::convert::Into;
// use self::rand::random;
use dbcluster::DBCluster;
use backend::DefaultHTTPBackend;

pub type Cluster = DBCluster<DefaultHTTPBackend>;

#[cfg(test)]
mod tests {
    use dbcluster::Nothing;
    use backend::Backend;
    use sql::QueryRunner;
    use super::error::{BackendError, CrateDBError};
    use super::DBCluster;
    use super::row::{Row, ByIndex};
    use std::io::Read;

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

        fn upload_blob(&self,
                       to: Option<String>,
                       bucket: &str,
                       sha1: &[u8],
                       f: &mut Read)
                       -> Result<(), BackendError> {
            Ok(())
        }

        fn delete_blob(&self,
                       to: Option<String>,
                       bucket: &str,
                       sha1: &[u8])
                       -> Result<(), BackendError> {
            Ok(())
        }
        fn fetch_blob(&self,
                      to: Option<String>,
                      bucket: &str,
                      sha1: &[u8])
                      -> Result<Box<Read>, BackendError> {
            Err(BackendError { response: "hello".to_string() })
        }
    }


    fn new_cluster(response: &str, failing: bool) -> DBCluster<MockBackend> {
        DBCluster::with_custom_backend(vec![], MockBackend::new(response.to_owned(), failing))
    }


    #[derive(Serialize)]
    struct TestObj {
        a: i32,
        b: String,
        c: f64,
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

        let result = cluster.query("insert into mytable (v1, v2) values (?, ?)",
                                   Some(Box::new((1,
                                                  TestObj {
                                       a: 1,
                                       b: "asd".to_string(),
                                       c: 3.14,
                                   }))));
        assert!(result.is_ok());
        let (t, result) = result.unwrap();
        assert_eq!(t, 0.206f64);
        assert_eq!(result.len(), 1);
        assert_eq!(rows.get(0).unwrap().as_string(0).unwrap(), "A".to_owned());
    }

    #[test]
    fn no_parameter_query() {
        let cluster = new_cluster("{\"cols\":[\"name\"],\"rows\":[[\"A\"]],\"rowcount\":1,\
                                       \"duration\":0.206}",
                                  false);
        let result = cluster.query("select name from mytable where a = 'hello'",
                                   None::<Box<Nothing>>);
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
        println!("here");
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
        let result = cluster.query("create table a(a string, b long)", None::<Box<Nothing>>);
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


        let result = cluster.query("select * from sys.nodes", None::<Box<Nothing>>);
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
