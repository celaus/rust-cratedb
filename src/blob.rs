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

use std::io::{Read, Seek};
use error::BlobError;
use dbcluster::DBCluster;
use backend::Backend;
use dbcluster::{Loadbalancing, EndpointType};
use common::sha1_digest;


pub struct BlobRef {
    pub sha1: Vec<u8>,
    pub table: String,
}


pub trait BlobContainer {
    ///
    /// Uploads an existing blob to the cluster.
    ///
    fn put<B: Read + Seek>(&self, table: String, blob: &mut B) -> Result<BlobRef, BlobError>;


    ///
    /// Deletes a blob on the cluster.
    ///
    fn delete(&self, blob: BlobRef) -> Result<(), BlobError>;

    ///
    /// Fetches an existing blob from the cluster.
    ///
    fn get(&self, blob: &BlobRef) -> Result<Box<Read>, BlobError>;
}



impl<T: Backend + Sized> BlobContainer for DBCluster<T> {
    fn put<B: Read + Seek>(&self, table: String, blob: &mut B) -> Result<BlobRef, BlobError> {
        match sha1_digest(blob) {
            Ok(sha1) => {
                let url = self.get_endpoint(EndpointType::Blob);
                let _ = self.backend.upload_blob(url, &table, &sha1, blob);
                Ok(BlobRef {
                    table: table,
                    sha1: sha1,
                })
            }
            Err(io) => Err(BlobError::Io(io)),
        }
    }


    fn delete(&self, blob: BlobRef) -> Result<(), BlobError> {
        let url = self.get_endpoint(EndpointType::Blob);
        self.backend.delete_blob(url, &blob.table, &blob.sha1).map_err(BlobError::Backend)
    }


    fn get(&self, blob: &BlobRef) -> Result<Box<Read>, BlobError> {
        let url = self.get_endpoint(EndpointType::Blob);
        self.backend.fetch_blob(url, &blob.table, &blob.sha1).map_err(BlobError::Backend)
    }
}