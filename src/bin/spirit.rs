use persona_spirit::{Result, SingleArgument, ordinary::Client};

fn main() -> Result<()> {
    let argument = SingleArgument::from_environment()?;
    Client::from_argument(argument)?.run()
}
