use std::fs;

use crate::{Error, Result};

/// The single NOTA argument a Spirit binary accepts, resolved to its
/// NOTA text. An argument beginning with `(` is inline NOTA; anything
/// else is a path to a NOTA file whose contents are the NOTA text.
///
/// This is the daemon-and-migration shared resolution of the
/// single-argument rule (`AGENTS.md` §"NOTA is the only argument
/// language"): every component binary takes exactly one argument, a NOTA
/// string or a path to a NOTA file.
pub struct SpiritArgument(signal_frame::SingleArgument);

impl SpiritArgument {
    pub fn into_nota_text(self) -> Result<String> {
        let value = self.0.as_str();
        if value.starts_with('(') {
            Ok(value.to_string())
        } else {
            fs::read_to_string(value).map_err(Error::input_output)
        }
    }
}

impl From<signal_frame::SingleArgument> for SpiritArgument {
    fn from(argument: signal_frame::SingleArgument) -> Self {
        Self(argument)
    }
}
