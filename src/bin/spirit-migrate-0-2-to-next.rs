use nota_codec::{Encoder, NotaEncode};
use persona_spirit::{Error, MigrationConfiguration, Result};
use signal_frame::SingleArgument;

fn main() -> Result<()> {
    let argument = SingleArgument::from_environment().map_err(Error::from)?;
    let outcome = MigrationConfiguration::from_argument(argument)?.migrate_v020_to_next()?;
    let mut encoder = Encoder::new();
    outcome
        .completed()
        .encode(&mut encoder)
        .map_err(Error::invalid_spirit_reply)?;
    println!("{}", encoder.into_string());
    Ok(())
}
