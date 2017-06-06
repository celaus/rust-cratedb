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

use std::error::Error;
use std::fmt::{self, Debug};
use std::io;
use std::cmp::PartialEq;
use self::hyper::Error as TransportError;


#[derive(Debug, PartialEq, Deserialize)]
pub struct CrateDBError {
    pub message: String,
    pub code: String,
    pub description: String,
}

impl CrateDBError {
    pub fn new<S1, S2>(message: S1, code: S2) -> CrateDBError
        where S1: Into<String>,
              S2: Into<String>
    {
        let c = code.into();
        let m = message.into();
        let desc = format!("Error [Code {}]: {}", c, m);
        CrateDBError {
            message: m,
            code: c,
            description: desc,
        }
    }
}


impl fmt::Display for CrateDBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Error for CrateDBError {
    fn description(&self) -> &str {
        &self.description
    }
}


#[derive(Debug)]
pub struct CrateDBConfigurationError {
    pub description: String,
}

impl Error for CrateDBConfigurationError {
    fn description(&self) -> &str {
        &self.description
    }
}

impl fmt::Display for CrateDBConfigurationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Debug::fmt(&self.description, f)
    }
}


#[derive(Debug)]
pub enum BackendError {
    Transport(TransportError),
    Io(io::Error),
    Custom { message: String },
}

#[derive(Debug)]
pub enum BlobError {
    Crate(CrateDBError),
    Io(io::Error),
    Backend(BackendError),
}