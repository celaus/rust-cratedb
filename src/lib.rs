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
mod common;


use dbcluster::DBCluster;
use backend::DefaultHTTPBackend;

pub type Cluster = DBCluster<DefaultHTTPBackend>;
pub type NoParams = sql::Nothing;

#[deprecated(since="1.0.0", note="Please use `NoParams`")]
pub type Nothing = NoParams;

#[cfg(test)]
mod tests {
    extern crate hex;
    use super::Nothing;
    use backend::Backend;
    use sql::QueryRunner;
    use blob::{BlobContainer, BlobRef};
    use super::error::{BackendError,BlobError, CrateDBError};
    use super::DBCluster;
    use super::row::{Row, ByIndex};
    use std::io::{Read, Cursor};
    use common::sha1_digest;
    use self::hex::FromHex;

    struct MockBackend {
        failing: bool,
        response: String,
        blobs: Vec<MockBlob>,
    }

    #[derive(PartialEq, Clone)]
    struct MockBlob {
        contents: Vec<u8>,
        sha1: Vec<u8>,
        bucket: String,
    }


    impl MockBackend {
        pub fn new(response: String, failing: bool, blobs: Vec<MockBlob>) -> MockBackend {
            MockBackend {
                failing: failing,
                response: response,
                blobs: blobs,
            }
        }
    }

    impl Backend for MockBackend {
        fn execute(&self, to: Option<String>, payload: String) -> Result<String, BackendError> {
            let _ = (to, payload);
            if !self.failing {
                return Ok(self.response.clone());
            } else {
                return Err(BackendError::Custom { message: self.response.clone() });
            }
        }

        fn upload_blob(&self,
                       to: Option<String>,
                       bucket: &str,
                       sha1: &[u8],
                       f: &mut Read)
                       -> Result<(), BackendError> {
            if !self.failing {
                let mut buffer = Vec::new();
                let _ = f.read_to_end(&mut buffer);
                let sha1_v = sha1.to_vec();

                let blob_pos = self.blobs
                    .binary_search_by(|e| e.sha1.cmp(&sha1_v))
                    .expect("blob not found");
                let blob = &self.blobs[blob_pos];
                assert_eq!(blob.sha1, sha1_v);
                assert_eq!(blob.bucket, bucket);
                assert_eq!(blob.contents, buffer);

                Ok(())
            } else {
                Err(BackendError::Custom { message: "Things failed".to_string() })
            }
        }

        fn delete_blob(&self,
                       to: Option<String>,
                       bucket: &str,
                       sha1: &[u8])
                       -> Result<(), BackendError> {
            if !self.failing {
                let sha1_v = sha1.to_vec();

                let blob_pos = self.blobs
                    .binary_search_by(|e| e.sha1.cmp(&sha1_v))
                    .expect("blob not found");
                let blob = &self.blobs[blob_pos];
                assert_eq!(blob.sha1, sha1_v);
                assert_eq!(blob.bucket, bucket);
                Ok(())
            } else {
                Err(BackendError::Custom { message: "Things failed".to_string() })
            }
        }

        fn fetch_blob(&self,
                      to: Option<String>,
                      bucket: &str,
                      sha1: &[u8])
                      -> Result<Box<Read>, BackendError> {
            if !self.failing {
                let sha1_v = sha1.to_vec();

                let blob_pos = self.blobs
                    .binary_search_by(|e| e.sha1.cmp(&sha1_v))
                    .expect("blob not found");
                let blob = &self.blobs[blob_pos];
                assert_eq!(blob.sha1, sha1_v);
                assert_eq!(blob.bucket, bucket);

                Ok(Box::new(Cursor::new(blob.contents.clone())))
            } else {
                Err(BackendError::Custom { message: "Things failed".to_string() })
            }
        }
    }


    fn new_cluster(response: &str, failing: bool) -> DBCluster<MockBackend> {
        new_cluster_with_blobs(response, failing, vec![])
    }
    fn new_cluster_with_blobs(response: &str,
                              failing: bool,
                              blobs: Vec<MockBlob>)
                              -> DBCluster<MockBackend> {
        DBCluster::with_custom_backend(vec![],
                                       MockBackend::new(response.to_owned(), failing, blobs))
    }


    #[derive(Serialize)]
    struct TestObj {
        a: i32,
        b: String,
        c: f64,
    }


    #[test]
    fn blob_upload() {
        let blob_a = vec![0x11, 0x12, 0x34, 0x53, 0x63, 0xAA, 0xFF];
        let bucket = "bucket".to_string();
        let expected_sha1 = sha1_digest(&mut Cursor::new(&blob_a)).unwrap();
        let blobs = vec![MockBlob {
                             sha1: expected_sha1.clone(),
                             contents: blob_a.clone(),
                             bucket: bucket.clone(),
                         }];
        let cluster = new_cluster_with_blobs("", false, blobs);

        let result = cluster
            .put(bucket.clone(), &mut Cursor::new(&blob_a))
            .unwrap();

        assert_eq!(result.sha1, expected_sha1);
        assert_eq!(result.table, bucket);
    }


    #[test]
    fn blob_download() {
        let blob_a = vec![0x11, 0x12, 0x34, 0x53, 0x63, 0xAA, 0xFF];
        let bucket = "bucket".to_string();
        let expected_sha1 = sha1_digest(&mut Cursor::new(&blob_a)).unwrap();
        let blobs = vec![MockBlob {
                             sha1: expected_sha1.clone(),
                             contents: blob_a.clone(),
                             bucket: bucket.clone(),
                         }];

        let blobref = BlobRef {
            sha1: expected_sha1.clone(),
            table: bucket.clone(),
        };

        let cluster = new_cluster_with_blobs("", false, blobs);

        let mut result = cluster.get(&blobref).unwrap();
        let mut buffer: Vec<u8> = vec![];
        let _ = result.read_to_end(&mut buffer);
        assert_eq!(buffer, blob_a);
    }

    #[test]
    fn blob_delete() {
        let blob_a = vec![0x11, 0x12, 0x34, 0x53, 0x63, 0xAA, 0xFF];
        let bucket = "bucket".to_string();
        let expected_sha1 = sha1_digest(&mut Cursor::new(&blob_a)).unwrap();
        let blobs = vec![MockBlob {
                             sha1: expected_sha1.clone(),
                             contents: blob_a.clone(),
                             bucket: bucket.clone(),
                         }];

        let blobref = BlobRef {
            sha1: expected_sha1.clone(),
            table: bucket.clone(),
        };

        let cluster = new_cluster_with_blobs("", false, blobs);

        assert!(cluster.delete(blobref).is_ok());
    }

    #[test]
    fn blob_list() {
        let sha1= "4a756ca07e9487f482465a99e8286abc86ba4dc7";
        let expected_sha1 = Vec::from_hex(sha1).unwrap();
        let bucket = "bucket".to_string();
        let blobref = BlobRef {
            sha1: expected_sha1.clone(),
            table: bucket.clone(),
        };

        let cluster = new_cluster_with_blobs(&format!("{{\"cols\":[\"digest\"],\"rows\":[[\"{}\"]],\"rowcount\":1,\"duration\":0.206}}", sha1), false, vec![]);
        
        let expected= vec![blobref.clone()];

        assert_eq!(cluster.list(bucket).unwrap(), expected);
    }
        #[test]
    fn error_blob_list() {
        let bucket = "bucket".to_string();
          let cluster = new_cluster_with_blobs(&format!("{{\"error\":{{\"message\":\"SQLActionException[TableUnknownException: Table 'blob.{}' unknown]\",\"code\":4041}}}}", bucket), false, vec![]);
        let error = cluster.list(bucket.as_ref()).unwrap_err();
        match error {
            BlobError::Crate(crate_error) => {
                        assert_eq!(crate_error.message, format!("SQLActionException[TableUnknownException: Table 'blob.{}' unknown]", bucket));
                                assert_eq!(crate_error.code, "4041");

            },
            _=> panic!("Unexpected Error was returned")
        }
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
