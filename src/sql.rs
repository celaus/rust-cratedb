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

extern crate serde;
extern crate hyper;
extern crate serde_json;
extern crate rand;

use dbcluster::DBCluster;
use self::serde_json::Value;
use self::serde::ser::Serialize;
use error::{CrateDBError, BackendError};
use rowiterator::RowIterator;
use std::collections::HashMap;
use std::convert::Into;
use backend::Backend;
use dbcluster::{Loadbalancing, EndpointType};

///
/// Empty struct to pass into argument lists for the Box to have a type.
///
#[derive(Serialize)]
pub struct Nothing {}


trait Executor {
    fn execute<SQL, S>(&self, sql: SQL, bulk: bool, params: Option<Box<S>>) -> String
        where SQL: Into<String>,
              S: Serialize;
}


pub trait QueryRunner {
    ///
    /// Runs a query. Returns the results and the duration
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use cratedb::Cluster;
    /// use cratedb::row::ByIndex;
    /// let node = "http://play.crate.io";
    /// let mut c: Cluster = Cluster::from_string(node).unwrap();
    /// let (elapsed, rows) = c.query("select hostname from sys.nodes", None::<Box<Nothing>>).unwrap();
    ///
    /// for r in rows {
    ///  println!("{}", r.as_string(0).unwrap());
    /// }
    /// ```
    fn query<SQL, S>(&self,
                     sql: SQL,
                     params: Option<Box<S>>)
                     -> Result<(f64, RowIterator), CrateDBError>
        where SQL: Into<String>,
              S: Serialize;


    /// Runs a query. Returns the results and the duration
    /// ```rust, ignore
    /// use doc::Cluster;
    /// use doc::row::ByIndex;
    /// let node = "http://play.crate.io";
    /// let mut c: Cluster = Cluster::from_string(node).unwrap();
    /// let (elapsed, rows) = c.bulk_query("select hostname from sys.nodes", Box::new("")).unwrap();
    ///
    /// for r in rows {
    ///  println!(r.as_string(0).unwrap());
    /// }
    /// ```
    fn bulk_query<SQL, S>(&self,
                          sql: SQL,
                          params: Box<S>)
                          -> Result<(f64, Vec<i64>), CrateDBError>
        where SQL: Into<String>,
              S: Serialize;
}


impl<T: Backend + Sized> Executor for DBCluster<T> {
    // Executes the query against the backend.
    fn execute<SQL, S>(&self, sql: SQL, bulk: bool, params: Option<Box<S>>) -> String
        where SQL: Into<String>,
              S: Serialize
    {
        let url = self.get_endpoint(EndpointType::SQL);
        let json_query = if bulk {
            json!({
                "stmt": sql.into(),
                "bulk_args": serde_json::to_value(params.unwrap()).unwrap()
                })
                    .to_string()
        } else if let Some(p) = params {
            json!({
                    "stmt": sql.into(),
                    "args": serde_json::to_value(p).unwrap()
                    })
                    .to_string()
        } else {
            json!({
                    "stmt": sql.into()
                    })
                    .to_string()
        };
        match self.backend.execute(url, json_query) {
            Ok(r) => r,
            Err(e) => e.description,
        }
    }
}

fn extract_error(data: &Value) -> CrateDBError {
    let message = data.pointer("/error/message").unwrap().as_str().unwrap();
    let code = data.pointer("/error/code")
        .unwrap()
        .as_i64()
        .unwrap()
        .to_string();
    CrateDBError::new(message, code)
}

impl<T: Backend + Sized> QueryRunner for DBCluster<T> {
    fn query<SQL, S>(&self,
                     sql: SQL,
                     params: Option<Box<S>>)
                     -> Result<(f64, RowIterator), CrateDBError>
        where SQL: Into<String>,
              S: Serialize
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
                       None => Err(extract_error(&data)),

                   };
        }
        Err(CrateDBError::new(format!("{}: {}", "Invalid JSON was returned", body), "500"))
    }



    fn bulk_query<SQL, S>(&self, sql: SQL, params: Box<S>) -> Result<(f64, Vec<i64>), CrateDBError>
        where SQL: Into<String>,
              S: Serialize
    {

        let body = self.execute(sql, true, Some(params));

        if let Ok(raw) = serde_json::from_str(&body) {
            let data: Value = raw;

            return match data.pointer("/cols") {
                       Some(_) => {
                           let bulk_results = data.pointer("/results").unwrap().as_array().unwrap();
                           let rowcounts = bulk_results
                               .into_iter()
                               .map(|v| v.pointer("/rowcount").unwrap().as_i64().unwrap())
                               .collect();
                           let duration = data.pointer("/duration").unwrap().as_f64().unwrap();
                           Ok((duration, rowcounts))
                       }
                       None => Err(extract_error(&data)),
                   };
        }
        Err(CrateDBError::new(format!("{}: {}", "Invalid JSON was returned", body), "500"))
    }
}
