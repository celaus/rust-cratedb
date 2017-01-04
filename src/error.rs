use std::error::Error;
use std::fmt::{self, Debug};

#[derive(Debug, PartialEq)]
pub struct CrateDBError {
    message: String,
    code: String,
    description: String,
}

impl CrateDBError {
    pub fn new<S1, S2>  (message: S1, code: S2) -> CrateDBError where S1: Into<String>, S2: Into<String> {
        let c = code.into();
        let m = message.into();
        let desc = format!("Error [Code {}]: {}", c, m);
        CrateDBError {
            message: m,
            code: c,
            description: desc
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
pub struct BackendError {
    pub response: String,
}

impl BackendError {
    pub fn new<S>(response: S) -> BackendError where S: Into<String> {
        BackendError {
            response: response.into(),
        }
    }
}

impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Debug::fmt(&self.response, f)
    }
}

impl Error for BackendError {
    fn description(&self) -> &str {
        &self.response
    }
}
