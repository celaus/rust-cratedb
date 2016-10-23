use std::error::Error;
use std::fmt::{self, Debug};

#[derive(Debug)]
pub struct CrateError {
    message: String,
    code: String,
    description: String,
}

impl CrateError {
    pub fn new (message: String, code: String) -> CrateError {
        let desc = format!("Error Code {}: {}", &code, &message);
        CrateError {
            message: message,
            code: code,
            description: desc
        }
    }
}

impl fmt::Display for CrateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Error for CrateError {
    fn description(&self) -> &str {
        &self.description
    }
}



#[derive(Debug)]
pub struct StringError {
    pub description: String,
}

impl fmt::Display for StringError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Debug::fmt(&self.description, f)
    }
}

impl Error for StringError {
    fn description(&self) -> &str {
        &self.description
    }
}


#[derive(Debug)]
pub struct BackendError {
    pub description: String,
}

impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Debug::fmt(&self.description, f)
    }
}

impl Error for BackendError {
    fn description(&self) -> &str {
        &self.description
    }
}
