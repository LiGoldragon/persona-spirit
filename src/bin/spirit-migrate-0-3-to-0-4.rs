use nota_next::NotaEncode;
use persona_spirit::{Error, MigrationConfiguration, Result};
use signal_frame::SingleArgument;

fn main() -> Result<()> {
    let argument = SingleArgument::from_environment().map_err(Error::from)?;
    let outcome = MigrationConfiguration::from_argument(argument)?.migrate_v030_to_v040()?;
    println!("{}", outcome.completed().to_nota());
    Ok(())
}
