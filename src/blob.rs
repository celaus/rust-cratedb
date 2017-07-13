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

extern crate hex;

use std::io::{Read, Seek};
use error::{BlobError, BackendError, CrateDBError};
use dbcluster::DBCluster;
use backend::{Backend, BackendResult};
use dbcluster::{Loadbalancing, EndpointType};
use common::sha1_digest;
use sql::{QueryRunner, Nothing as NoParams};
use row::ByIndex;
use self::hex::FromHex;


///
/// A reference to a server-side blob. Basically contains the SHA1 hash and the table.
///
#[derive(Debug, Clone, PartialEq)]
pub struct BlobRef {
    /// SHA1 byte vector that identifies the blob on the server.
    pub sha1: Vec<u8>,

    /// Table/bucket where the blob is located on the server.
    pub table: String,
}


///
/// Trait for interfacing with CrateDB's BLOB features.
///
pub trait BlobContainer {
    ///
    /// Fetches a list of [BlobRefs] from the provided table.
    ///
    /// # Errors
    /// Errors occur when a cluster is unreachable, the table doesn't exist or other conditions appear: [Crate.io docs](https://crate.io/docs/reference/blob.html)
    ///
    /// # Examples
    /// ```rust,ignore
    /// use blob::BlobContainer;
    /// let _ = c.query("create blob table my_blob_table", None::<Box<NoParams>>).unwrap();
    /// for blob_ref in c.list("my_blob_table").unwrap() {
    ///   println!("{:?}", blob_ref);
    /// }
    /// ```
    ///
    fn list<TBL: Into<String>>(&self, table: TBL) -> Result<Vec<BlobRef>, BlobError>;


    ///
    /// Uploads an existing blob to the cluster.
    ///
    /// # Errors
    /// Errors occur when a cluster is unreachable, the blob already exists, or other conditions appear: [Crate.io docs](https://crate.io/docs/reference/blob.html)
    ///
    /// # Examples
    /// ```rust,ignore
    /// use sql::QueryRunner;
    /// use blob::BlobContainer;
    /// let _ = c.query("create blob table my_blob_table", None::<Box<NoParams>>).unwrap();
    /// let myblob: Vec<u8> = iter::repeat(0xA).take(1024).collect();
    /// let r = c.put("my_blob_table", &mut Cursor::new(&myblob)).unwrap();
    /// println!("Uploaded BLOB: {:?}", r);
    /// ```
    ///
    fn put<TBL: Into<String>, B: Read + Seek>(&self,
                                              table: TBL,
                                              blob: &mut B)
                                              -> Result<BlobRef, BlobError>;


    ///
    /// Deletes a blob on the cluster using a [BlobRef] obtained from uploading.
    ///
    /// # Errors
    /// Errors occur when a cluster is unreachable, the blob already exists, or other conditions appear: [Crate.io docs](https://crate.io/docs/reference/blob.html)
    ///
    /// # Examples
    /// ```rust,ignore
    /// use blob::BlobContainer;
    /// let _ = c.delete(my_blob_ref);
    /// ```
    ///
    fn delete(&self, blob: BlobRef) -> Result<(), BlobError>;

    ///
    /// Fetches an existing blob from the cluster.
    ///
    /// # Errors
    /// Errors occur when a cluster is unreachable, the blob already exists, or other conditions appear: [Crate.io docs](https://crate.io/docs/reference/blob.html)
    ///
    /// # Examples
    /// ```rust,ignore
    /// use blob::BlobContainer;
    /// let _ = c.get(&my_blob_ref);
    /// ```
    ///
    fn get(&self, blob: &BlobRef) -> Result<Box<Read>, BlobError>;
}



impl<T: Backend + Sized> BlobContainer for DBCluster<T> {
    fn put<TBL: Into<String>, B: Read + Seek>(&self,
                                              table: TBL,
                                              blob: &mut B)
                                              -> Result<BlobRef, BlobError> {
        match sha1_digest(blob) {
            Ok(sha1) => {
                let url = self.get_endpoint(EndpointType::Blob);
                let table = table.into();
                match self.backend
                          .upload_blob(url, &table, &sha1, blob)
                          .map_err(BlobError::Transport) {
                    Ok(status) => {
                        match status {
                            BackendResult::Ok => {
                                Ok(BlobRef {
                                       table: table,
                                       sha1: sha1,
                                   })
                            }
                            BackendResult::NotFound => {
                                Err(BlobError::Action(CrateDBError::new("Could not upload BLOB. Not found.",
                                                                        "404")))
                            }
                            BackendResult::NotAuthorized => {
                                Err(BlobError::Action(CrateDBError::new("Could not upload BLOB: Not authorized.",
                                                                        "403")))
                            }
                            BackendResult::Timeout => {
                                Err(BlobError::Action(CrateDBError::new("Could not upload BLOB. Timed out.",
                                                                        "408")))
                            }
                            BackendResult::Error => {
                                Err(BlobError::Action(CrateDBError::new("Could not upload BLOB. Server error.",
                                                                        "500")))
                            }
                        }
                    }
                    Err(e) => Err(e),
                }
            }   
            Err(io) => Err(BlobError::Transport(BackendError::from_io(io))),
        }
    }



    fn delete(&self, blob: BlobRef) -> Result<(), BlobError> {
        let url = self.get_endpoint(EndpointType::Blob);
        match self.backend
                  .delete_blob(url, &blob.table, &blob.sha1)
                  .map_err(BlobError::Transport) {
            Ok(status) => {
                match status {
                    BackendResult::Ok => Ok(()),
                    BackendResult::NotFound => {
                        Err(BlobError::Action(CrateDBError::new("Could not delete BLOB. Not found.",
                                                                "404")))
                    }
                    BackendResult::NotAuthorized => {
                        Err(BlobError::Action(CrateDBError::new("Could not delete BLOB: Not authorized.",
                                                                "403")))
                    }
                    BackendResult::Timeout => {
                        Err(BlobError::Action(CrateDBError::new("Could not delete BLOB. Timed out.",
                                                                "408")))
                    }
                    BackendResult::Error => {
                        Err(BlobError::Action(CrateDBError::new("Could not delete BLOB. Server error.",
                                                                "500")))
                    }
                }
            }
            Err(e) => Err(e),
        }
    }


    fn get(&self, blob: &BlobRef) -> Result<Box<Read>, BlobError> {
        let url = self.get_endpoint(EndpointType::Blob);
        match self.backend
                  .fetch_blob(url, &blob.table, &blob.sha1)
                  .map_err(BlobError::Transport) {
            Ok((status, content)) => {
                match status {
                    BackendResult::Ok => Ok(content),
                    BackendResult::NotFound => {
                        Err(BlobError::Action(CrateDBError::new("Could not fetch BLOB. Not found.",
                                                                "404")))
                    }
                    BackendResult::NotAuthorized => {
                        Err(BlobError::Action(CrateDBError::new("Could not fetch BLOB: Not authorized.",
                                                                "403")))
                    }
                    BackendResult::Timeout => {
                        Err(BlobError::Action(CrateDBError::new("Could not fetch BLOB. Timed out.",
                                                                "408")))
                    }
                    BackendResult::Error => {
                        Err(BlobError::Action(CrateDBError::new("Could not fetch BLOB. Server error.",
                                                                "500")))
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    fn list<TBL: Into<String>>(&self, table: TBL) -> Result<Vec<BlobRef>, BlobError> {
        let table_name = table.into();
        match self.query(format!("select digest from blob.{}", table_name),
                         None::<Box<NoParams>>) {
            Ok((_, rows)) => {
                let mut blob_refs = Vec::with_capacity(rows.len());
                for row in rows {
                    if let Some(digest_str) = row.as_string(0) {
                        if let Ok(digest) = Vec::from_hex(digest_str) {
                            blob_refs.push(BlobRef {
                                               sha1: digest,
                                               table: table_name.clone(),
                                           });
                        }
                    }
                }
                Ok(blob_refs)
            }
            Err(e) => Err(BlobError::Action(e)),
        }
    }
}