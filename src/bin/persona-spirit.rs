use persona_spirit::{Result, SingleArgument, SpiritClient};

fn main() -> Result<()> {
    let argument = SingleArgument::from_environment()?;
    SpiritClient::from_argument(argument)?.run()
}
