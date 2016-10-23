extern crate serde_json;

use row::Row;
use std::collections::HashMap;
use self::serde_json::Value;



pub struct RowIterator  {
    rows:  Vec<Value>,
    header: HashMap<String, usize>,
}

impl  RowIterator  {
    pub fn new(mut rows: Vec<Value>, header: HashMap<String, usize>) -> RowIterator  {
        rows.reverse();
        RowIterator {
            rows: rows,
            header: header,
        }
    }
}

impl  Iterator for RowIterator  {
    type Item = Row;

    fn next(&mut self) -> Option<Row> {
        match self.rows.pop(){
            Some(i) => Some(Row::new(i.as_array().unwrap().to_vec(), self.header.clone())),
            _ => None
        }

    }
}
