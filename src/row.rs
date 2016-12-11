use serde_json::Value;
use std::collections::HashMap;

pub struct Row {
    wrapped: Vec<Value>,
    columns: HashMap<String, usize>,
}

pub trait ByIndex {
    fn as_i64(&self, idx: usize) -> Option<i64>;
    fn as_u64(&self, idx: usize) -> Option<u64>;
    fn as_f64(&self, idx: usize) -> Option<f64>;
    fn as_bool(&self, idx: usize) -> Option<bool>;
    fn as_string(&self, idx: usize) -> Option<String>;
}


pub trait ByColumnName {
    fn as_i64(&self, col: &String) -> Option<i64>;
    fn as_u64(&self, col: &String) -> Option<u64>;
    fn as_f64(&self, col: &String) -> Option<f64>;
    fn as_bool(&self, col: &String) -> Option<bool>;
    fn as_string(&self, col: &String) -> Option<String>;
}

impl Row {
    pub fn new(wrapped: Vec<Value>, headers: HashMap<String, usize>) -> Row {
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
}
