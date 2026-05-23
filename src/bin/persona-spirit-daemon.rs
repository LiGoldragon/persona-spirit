use persona_spirit::{DaemonRuntime, Error, Result};
use signal_frame::SingleArgument;

fn main() -> Result<()> {
    let argument = SingleArgument::from_environment().map_err(Error::from)?;
    DaemonRuntime::from_argument(argument)?.run()
}
