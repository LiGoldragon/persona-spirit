use persona_spirit::{Error, SingleArgument};

#[test]
fn persona_spirit_binary_accepts_exactly_one_argument() {
    let argument = SingleArgument::from_arguments([
        "persona-spirit".to_string(),
        "(PsycheStatement \"capture this intent\")".to_string(),
    ])
    .expect("single argument accepted");

    assert_eq!(
        argument.as_str(),
        "(PsycheStatement \"capture this intent\")"
    );
}

#[test]
fn persona_spirit_binary_rejects_missing_argument() {
    let error = SingleArgument::from_arguments(["persona-spirit".to_string()]).unwrap_err();

    assert_eq!(
        error,
        Error::WrongArgumentCount {
            program: "persona-spirit".to_string(),
            found: 0,
        }
    );
}

#[test]
fn persona_spirit_binary_rejects_extra_argument() {
    let error = SingleArgument::from_arguments([
        "persona-spirit".to_string(),
        "(PsycheStatement \"one\")".to_string(),
        "(PsycheStatement \"two\")".to_string(),
    ])
    .unwrap_err();

    assert_eq!(
        error,
        Error::WrongArgumentCount {
            program: "persona-spirit".to_string(),
            found: 2,
        }
    );
}

#[test]
fn persona_spirit_binary_rejects_flag_style_argument() {
    let error =
        SingleArgument::from_arguments(["persona-spirit".to_string(), "--help".to_string()])
            .unwrap_err();

    assert_eq!(
        error,
        Error::FlagArgument {
            program: "persona-spirit".to_string(),
            argument: "--help".to_string(),
        }
    );
}
