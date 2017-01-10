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
extern crate serde;

use serde_json::Value;
use std::collections::HashMap;
use self::serde::de::Deserialize;
use std::rc::Rc;

///
/// A row in a result set of a CrateDB query. Provides
/// accessors for the contained data.
///
pub struct Row {
    wrapped: Vec<Value>,
    columns: Rc<HashMap<String, usize>>,
}

///
/// Get row values by their index.
///
pub trait ByIndex {
    fn as_i64(&self, idx: usize) -> Option<i64>;
    fn as_u64(&self, idx: usize) -> Option<u64>;
    fn as_f64(&self, idx: usize) -> Option<f64>;
    fn as_bool(&self, idx: usize) -> Option<bool>;
    fn as_string(&self, idx: usize) -> Option<String>;
    fn as_array<T: Deserialize>(&self, idx: usize) -> Option<Vec<T>>;
}

///
/// Get row values by their name.
///
pub trait ByColumnName {
    fn as_i64(&self, col: &String) -> Option<i64>;
    fn as_u64(&self, col: &String) -> Option<u64>;
    fn as_f64(&self, col: &String) -> Option<f64>;
    fn as_bool(&self, col: &String) -> Option<bool>;
    fn as_string(&self, col: &String) -> Option<String>;
    fn as_array<T: Deserialize>(&self, col: &String) -> Option<Vec<T>>;
}

impl Row {
    pub fn new(wrapped: Vec<Value>, headers: Rc<HashMap<String, usize>>) -> Row {
        Row {
            wrapped: wrapped,
            columns: headers,
        }
    }
}

impl ByIndex for Row {
    fn as_string(&self, idx: usize) -> Option<String> {
        return match self.wrapped.get(idx).unwrap().as_str() {
            Some(r) => Some(r.to_string()),
            _ => None,
        };
    }

    fn as_i64(&self, idx: usize) -> Option<i64> {
        return self.wrapped.get(idx).unwrap().as_i64();
    }

    fn as_u64(&self, idx: usize) -> Option<u64> {
        return self.wrapped.get(idx).unwrap().as_u64();
    }

    fn as_f64(&self, idx: usize) -> Option<f64> {
        return self.wrapped.get(idx).unwrap().as_f64();
    }

    fn as_bool(&self, idx: usize) -> Option<bool> {
        return self.wrapped.get(idx).unwrap().as_bool();
    }

    fn as_array<T: Deserialize>(&self, idx: usize) -> Option<Vec<T>> {
        match self.wrapped.get(idx).unwrap().as_array() {
            Some(v) => {
                Some(v.into_iter().map(|e| serde_json::from_value(e.clone()).unwrap()).collect())
            }
            None => None,
        }

    }
}

impl ByColumnName for Row {
    fn as_string(&self, col: &String) -> Option<String> {
        return match self.columns.get(col) {
            Some(idx) => ByIndex::as_string(self, *idx),
            None => None,
        };
    }

    fn as_i64(&self, col: &String) -> Option<i64> {
        return match self.columns.get(col) {
            Some(idx) => ByIndex::as_i64(self, *idx),
            None => None,
        };
    }

    fn as_u64(&self, col: &String) -> Option<u64> {
        return match self.columns.get(col) {
            Some(idx) => ByIndex::as_u64(self, *idx),
            None => None,
        };
    }

    fn as_f64(&self, col: &String) -> Option<f64> {
        return match self.columns.get(col) {
            Some(idx) => ByIndex::as_f64(self, *idx),
            None => None,
        };
    }

    fn as_bool(&self, col: &String) -> Option<bool> {
        return match self.columns.get(col) {
            Some(idx) => ByIndex::as_bool(self, *idx),
            None => None,
        };
    }

    fn as_array<T: Deserialize>(&self, col: &String) -> Option<Vec<T>> {
        return match self.columns.get(col) {
            Some(idx) => ByIndex::as_array(self, *idx),
            None => None,
        };
    }
}
#[cfg(test)]
mod tests {
    extern crate serde_json;
    use super::{Row, ByColumnName, ByIndex};
    use std::collections::HashMap;
    use std::rc::Rc;

    fn get_row() -> Row {
        let mut v_obj = HashMap::new();
        v_obj.insert("hello", "world");

        let mut headers = HashMap::new();
        headers.insert("str".to_owned(), 0usize);
        headers.insert("uint".to_owned(), 1usize);
        headers.insert("float".to_owned(), 2usize);
        headers.insert("bool".to_owned(), 3usize);
        headers.insert("sint".to_owned(), 4usize);
        headers.insert("array".to_owned(), 5usize);
        headers.insert("array_of_arrays".to_owned(), 6usize);


        let v = vec![serde_json::to_value("hello"),
                     serde_json::to_value(1234),
                     serde_json::to_value(3.141528),
                     serde_json::to_value(true),
                     serde_json::to_value(-1234),
                     serde_json::to_value(vec![1, 2, 3, 4]),
                     serde_json::to_value(vec![vec![1, 1], vec![2, 2]])];

        return Row::new(v, Rc::new(headers));
    }

    #[test]
    fn by_column_name() {
        let row = get_row();
        assert_eq!(ByColumnName::as_string(&row, &"str".to_owned()),
                   Some("hello".to_owned()));
        assert_eq!(ByColumnName::as_u64(&row, &"uint".to_owned()),
                   Some(1234u64));
        assert_eq!(ByColumnName::as_f64(&row, &"float".to_owned()),
                   Some(3.141528));
        assert_eq!(ByColumnName::as_bool(&row, &"bool".to_owned()), Some(true));
        assert_eq!(ByColumnName::as_i64(&row, &"sint".to_owned()),
                   Some(-1234i64));
        assert_eq!(ByColumnName::as_array(&row, &"array".to_owned()),
                   Some(vec![1, 2, 3, 4]));
        assert_eq!(ByColumnName::as_array(&row, &"array_of_arrays".to_owned()),
                   Some(vec![vec![1, 1], vec![2, 2]]));

    }

    #[test]
    fn by_index() {
        let row = get_row();
        assert_eq!(ByIndex::as_string(&row, 0), Some("hello".to_owned()));
        assert_eq!(ByIndex::as_u64(&row, 1), Some(1234u64));
        assert_eq!(ByIndex::as_f64(&row, 2), Some(3.141528));
        assert_eq!(ByIndex::as_bool(&row, 3), Some(true));
        assert_eq!(ByIndex::as_i64(&row, 4), Some(-1234i64));
        assert_eq!(ByIndex::as_array(&row, 5), Some(vec![1, 2, 3, 4]));
        assert_eq!(ByIndex::as_array(&row, 6),
                   Some(vec![vec![1, 1], vec![2, 2]]));
    }
}
