use persona_spirit::{Error, SingleArgument, SpiritClient};

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

#[test]
fn persona_spirit_client_type_checks_psyche_statement() {
    let argument = SingleArgument::from_arguments([
        "persona-spirit".to_string(),
        "(PsycheStatement \"capture this intent\")".to_string(),
    ])
    .expect("single argument accepted");

    let reply = SpiritClient::from_argument(argument)
        .reply_text()
        .expect("request type checked");

    assert_eq!(
        reply,
        "(SpiritRequestUnimplemented PsycheStatement NotBuiltYet)"
    );
}

#[test]
fn persona_spirit_client_type_checks_intent_entry_with_restatements() {
    let argument = SingleArgument::from_arguments([
        "persona-spirit".to_string(),
        "(IntentEntry workspace Decision \"summary only\" \"current implementation context\" Maximum [(IntentVerbatim \"2026-05-19T13:08:11Z\" \"first statement\") (IntentVerbatim \"2026-05-19T13:12:00Z\" \"restated statement\")])".to_string(),
    ])
    .expect("single argument accepted");

    let reply = SpiritClient::from_argument(argument)
        .reply_text()
        .expect("intent entry type checked");

    assert_eq!(
        reply,
        "(SpiritRequestUnimplemented IntentEntry NotBuiltYet)"
    );
}

#[test]
fn persona_spirit_client_rejects_unknown_record_shape() {
    let argument = SingleArgument::from_arguments([
        "persona-spirit".to_string(),
        "(UnknownIntent workspace)".to_string(),
    ])
    .expect("single argument accepted");

    let error = SpiritClient::from_argument(argument)
        .reply_text()
        .unwrap_err();

    assert!(matches!(error, Error::InvalidSpiritRequest { .. }));
}
