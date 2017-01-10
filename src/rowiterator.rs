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


extern crate serde_json;

use row::Row;
use std::collections::HashMap;
use self::serde_json::Value;
use std::iter::Map;
use std::rc::Rc;
use std::slice::Iter;

#[derive(Debug)]
pub struct RowIterator<'a>  {
    rows:  Vec<Value>,
    header: Rc<HashMap<String, usize>>,
    iter: Map<Iter<&'a Value>, Row>
}

impl  <'a>RowIterator<'a>  {
    pub fn new(rows: Vec<Value>, header: HashMap<String, usize>) -> RowIterator  {
        let headers = Rc::new(header);
        RowIterator {
            iter: rows.iter().map(|r| Row::new(r.as_array().unwrap().to_vec(), headers.clone())),
            rows: rows,
            header: headers,
        }
    }
    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

impl  <'a>Iterator for RowIterator<'a>  {
    type Item = Row;

    fn next(&mut self) -> Option<Row> {
        self.iter.next()
    }
}
