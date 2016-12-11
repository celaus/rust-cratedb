use std::error::Error;
use std::fmt::{self, Debug};

#[derive(Debug, PartialEq)]
pub struct CrateDBError {
    message: String,
    code: String,
    description: String,
}

impl CrateDBError {
    pub fn new  (message: String, code: String) -> CrateDBError {
        let desc = format!("Error Code {}: {}", &code, &message);
        CrateDBError {
            message: message,
            code: code,
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
    pub fn new(response: String) -> BackendError {
        BackendError {
            response: response,
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
