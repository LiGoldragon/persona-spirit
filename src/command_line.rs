use std::{io::Write, process::ExitCode};

use signal_frame::{
    ClientShape, CommandLineError, CommandLineSocket, Request, RequestHead, RequestInput,
    RequestText, SingleArgument,
};
use signal_persona_spirit::{Operation, OutputStream, OutputTarget};

type WorkingFrame = signal_persona_spirit::Frame;
type OwnerFrame = owner_signal_persona_spirit::Frame;

pub struct CommandLine {
    client: ClientShape<WorkingFrame, OwnerFrame>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandLineReply {
    text: String,
    stream: OutputStream,
}

struct RequestOutputStream {
    stream: OutputStream,
}

impl CommandLine {
    pub fn from_binary_name(binary_name: &str) -> Self {
        Self::new(ClientShape::<WorkingFrame, OwnerFrame>::from_binary_name(
            binary_name,
        ))
    }

    pub fn new(client: ClientShape<WorkingFrame, OwnerFrame>) -> Self {
        Self { client }
    }

    pub fn run_from_environment(binary_name: &str) -> ExitCode {
        Self::from_binary_name(binary_name).run_environment()
    }

    pub fn run_environment(&self) -> ExitCode {
        match SingleArgument::from_environment() {
            Ok(argument) => match self.run_argument(argument) {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("{error}");
                    error.exit_code()
                }
            },
            Err(error) => {
                let error = CommandLineError::from(error);
                eprintln!("{error}");
                error.exit_code()
            }
        }
    }

    pub fn run_argument(&self, argument: SingleArgument) -> Result<(), CommandLineError> {
        self.reply(argument)?.write()
    }

    pub fn reply(&self, argument: SingleArgument) -> Result<CommandLineReply, CommandLineError> {
        let stream = self.stream_for_argument(&argument)?;
        let text = self.client.reply_text(argument)?;
        Ok(CommandLineReply::new(text, stream))
    }

    fn stream_for_argument(
        &self,
        argument: &SingleArgument,
    ) -> Result<OutputStream, CommandLineError> {
        let text = RequestInput::new(argument.clone()).text()?;
        let head = RequestHead::from_text(&text)?;
        match head.route::<Operation, owner_signal_persona_spirit::Operation>()? {
            CommandLineSocket::Working => {
                let request = RequestText::<Operation>::new(text).decode_request()?;
                Ok(RequestOutputStream::from_working_request(&request).into_stream())
            }
            CommandLineSocket::Owner => Ok(OutputStream::StandardOutput),
        }
    }
}

impl CommandLineReply {
    pub fn new(text: String, stream: OutputStream) -> Self {
        Self { text, stream }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn stream(&self) -> OutputStream {
        self.stream
    }

    pub fn write(self) -> Result<(), CommandLineError> {
        match self.stream {
            OutputStream::StandardOutput => self.write_to(std::io::stdout().lock()),
            OutputStream::StandardError => self.write_to(std::io::stderr().lock()),
        }
    }

    fn write_to(self, mut writer: impl Write) -> Result<(), CommandLineError> {
        writeln!(writer, "{}", self.text).map_err(|error| CommandLineError::InputOutput {
            reason: error.to_string(),
        })
    }
}

impl RequestOutputStream {
    fn from_working_request(request: &Request<Operation>) -> Self {
        let mut stream = OutputStream::StandardOutput;
        for payload in request.payloads() {
            if Self::from_operation(payload).stream == OutputStream::StandardError {
                stream = OutputStream::StandardError;
            }
        }
        Self { stream }
    }

    fn from_operation(operation: &Operation) -> Self {
        let stream = match operation {
            Operation::CollectRemovalCandidates(collection) => match &collection.output_target {
                OutputTarget::Print(stream) => *stream,
                OutputTarget::ArchiveDatabase(_) => OutputStream::StandardOutput,
            },
            _ => OutputStream::StandardOutput,
        };
        Self { stream }
    }

    fn into_stream(self) -> OutputStream {
        self.stream
    }
}
