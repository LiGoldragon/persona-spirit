use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingleArgument {
    value: String,
}

impl SingleArgument {
    pub fn from_arguments<I>(arguments: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut iterator = arguments.into_iter();
        let program = iterator.next().unwrap_or_else(|| "spirit".to_string());
        let values: Vec<String> = iterator.collect();
        Self::from_program_and_values(program, values)
    }

    pub fn from_environment() -> Result<Self> {
        Self::from_arguments(std::env::args())
    }

    pub fn from_program_and_values(program: String, values: Vec<String>) -> Result<Self> {
        match values.as_slice() {
            [value] if value.starts_with("--") => Err(Error::FlagArgument {
                program,
                argument: value.clone(),
            }),
            [value] => Ok(Self {
                value: value.clone(),
            }),
            _ => Err(Error::WrongArgumentCount {
                program,
                found: values.len(),
            }),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub fn into_string(self) -> String {
        self.value
    }
}
