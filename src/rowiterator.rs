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
use std::rc::Rc;

#[derive(Debug)]
pub struct RowIterator {
    rows: Vec<Value>,
    header: Rc<HashMap<String, usize>>,
}

impl RowIterator {
    pub fn new(mut rows: Vec<Value>, header: HashMap<String, usize>) -> RowIterator {
        let headers = Rc::new(header);
        rows.reverse();
        RowIterator {
            rows: rows,
            header: headers,
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

impl Iterator for RowIterator {
    type Item = Row;

    fn next(&mut self) -> Option<Row> {
        match self.rows.pop() {
            Some(i) => Some(Row::new(i.as_array().unwrap().to_vec(), self.header.clone())),
            _ => None,
        }
    }
}
