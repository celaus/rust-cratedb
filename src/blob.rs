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

extern crate ring;
use self::ring::digest;
use std::io::Read;
use error::BlobError;
use dbcluster::DBCluster;
use backend::Backend;
use dbcluster::{Loadbalancing, EndpointType};


pub struct BlobRef {
    sha1: Vec<u8>,
    table: String,
}


pub trait BlobContainer {
    ///
    /// Uploads an existing blob to the cluster.
    ///
    fn put(&self, table: String, blob: &mut Read) -> Result<BlobRef, BlobError>;


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
    fn put(&self, table: String, blob: &mut Read) -> Result<BlobRef, BlobError> {
        let mut buffer = [0; 1024 * 100]; // 100 kb buffer size
        let mut ctx = digest::Context::new(&digest::SHA1);
        loop {
            match blob.read(&mut buffer[..]) {
                Ok(n) if n > 0 => ctx.update(&buffer),
                Err(e) => return Err(BlobError::Io(e)),
                _ => break,
            };
        }
        let sha1 = ctx.finish();
        let url = self.get_endpoint(EndpointType::Blob);
        let _ = self.backend.upload_blob(url, &table, sha1.as_ref(), blob);
        Ok(BlobRef {
            table: table,
            sha1: sha1.as_ref().to_vec(),
        })
    }


    fn delete(&self, blob: BlobRef) -> Result<(), BlobError> {
        let url = self.get_endpoint(EndpointType::Blob);
        self.backend.delete_blob(url, &blob.table, &blob.sha1).map_err(|e| BlobError::Backend(e))
    }


    fn get(&self, blob: &BlobRef) -> Result<Box<Read>, BlobError> {
        let url = self.get_endpoint(EndpointType::Blob);
        self.backend.fetch_blob(url, &blob.table, &blob.sha1).map_err(|e| BlobError::Backend(e))
    }
}