use persona_spirit::{DaemonRuntime, Result, SingleArgument};

fn main() -> Result<()> {
    let argument = SingleArgument::from_environment()?;
    DaemonRuntime::from_argument(argument).run()
}
