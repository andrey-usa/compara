//! The one error type the library returns.

use std::borrow::Cow;
use std::fmt;

/// Anything that stops a comparison: a missing file, a key that is not in both
/// headers, an engine that cannot run.
pub struct Error {
    /// Borrowed only for the refusal that had no room left to write its
    /// own ([`crate::alloc::refuse`]).
    message: Cow<'static, str>,
    /// Set when a reader is saying *not mine* -- a codec, a page version or a
    /// column type it does not handle -- rather than *this file is wrong*.
    ///
    /// The difference matters because this binary carries two Parquet readers.
    /// The columnar one is the fast path and reads a deliberately narrow slice
    /// of the format; `turbo` decodes pages into rows and reads far more of it.
    /// A refusal from the first is a reason to try the second, and for a while
    /// it was not: a zstd file was refused outright by a binary that could read
    /// it, with `--engine turbo` ignored on that path. [`crate::engine`] reads
    /// this flag to route around that.
    unsupported: bool,
    /// Set when an allocation was refused: the comparison needed more memory
    /// than it could have. Inside a container that is the limit it was given
    /// (see `crate::memory::contain`), and `auto` answers a CSV pair that hit
    /// it with the engine that spills to disk instead.
    out_of_memory: bool,
    /// Set when the inputs or the options are what is wrong (a key column
    /// that isn't there, two columns of one name): the user's to fix, said
    /// as it is, not as an engine failing.
    input: bool,
    /// What to do about it, shown after the message. Kept apart so a caller
    /// that does something about it itself can say the first half alone.
    hint: String,
}

impl Error {
    pub fn new(message: impl Into<String>) -> Self {
        Error {
            message: Cow::Owned(message.into()),
            unsupported: false,
            out_of_memory: false,
            input: false,
            hint: String::new(),
        }
    }

    /// The inputs or options are wrong. See [`Error::input`].
    pub fn input(message: impl Into<String>) -> Self {
        Error {
            input: true,
            ..Error::new(message)
        }
    }

    /// A reader declining a file it does not handle. See [`Error::unsupported`].
    pub fn unsupported(message: impl Into<String>) -> Self {
        Error {
            unsupported: true,
            ..Error::new(message)
        }
    }

    /// A reader declining a file for what the file is, with what to do
    /// about it: an input the user can change, and one another reader may
    /// take. Both [`Error::is_unsupported`] and [`Error::is_input`].
    pub fn declined(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Error {
            unsupported: true,
            input: true,
            hint: hint.into(),
            ..Error::new(message)
        }
    }

    /// The same error without its hint, for a caller about to act on it.
    pub fn without_hint(self) -> Self {
        Error {
            hint: String::new(),
            ..self
        }
    }

    /// The same error with `hint` in place of its own.
    pub fn with_hint(self, hint: impl Into<String>) -> Self {
        Error {
            hint: hint.into(),
            ..self
        }
    }

    /// An allocation the comparison could not have. See [`Error::out_of_memory`].
    pub fn out_of_memory(message: impl Into<String>) -> Self {
        Error {
            out_of_memory: true,
            ..Error::new(message)
        }
    }

    /// [`Error::out_of_memory`] with a message that costs no allocation, for
    /// a refusal made where none can be had.
    pub(crate) fn out_of_memory_static(message: &'static str) -> Self {
        Error {
            message: Cow::Borrowed(message),
            unsupported: false,
            out_of_memory: true,
            input: false,
            hint: String::new(),
        }
    }

    /// Whether this is a reader declining rather than a file being wrong.
    pub fn is_unsupported(&self) -> bool {
        self.unsupported
    }

    /// Whether the comparison ran out of the memory it was allowed.
    pub fn is_out_of_memory(&self) -> bool {
        self.out_of_memory
    }

    /// Whether the inputs or the options are what is wrong.
    pub fn is_input(&self) -> bool {
        self.input
    }

    /// `prefix` before the message, unless the inputs are what is wrong: "the
    /// turbo engine failed: key column x is not in B" says an engine broke
    /// when nothing did.
    pub fn context_unless_input(self, prefix: impl fmt::Display) -> Self {
        if self.input {
            self
        } else {
            self.context(prefix)
        }
    }

    /// The same error with `prefix` before its message, and its kind kept.
    pub fn context(self, prefix: impl fmt::Display) -> Self {
        Error {
            message: Cow::Owned(format!("{prefix}{}", self.message)),
            ..self
        }
    }

    /// The same error with every `from` in its message read as `to`, and its
    /// kind kept: an engine names the file it read, which for standard input
    /// or a decompressed copy is a temporary file the user never saw.
    pub fn renamed(self, from: &str, to: &str) -> Self {
        if from.is_empty() || from == to {
            return self;
        }
        Error {
            message: Cow::Owned(self.message.replace(from, to)),
            hint: self.hint.replace(from, to),
            ..self
        }
    }

    /// The same error with `suffix` after its message, and its kind kept.
    pub fn context_after(self, suffix: impl fmt::Display) -> Self {
        Error {
            message: Cow::Owned(format!("{}{suffix}", self.message)),
            ..self
        }
    }
}

/// Said with every URL in it shown without a credential ([`crate::redact`]):
/// a message can name the URL a request went to, which a redirect may have
/// signed, or a library's own text can.
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&crate::redact::urls(&self.message))?;
        f.write_str(&crate::redact::urls(&self.hint))
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("message", &crate::redact::urls(&self.message))
            .field("unsupported", &self.unsupported)
            .field("out_of_memory", &self.out_of_memory)
            .field("input", &self.input)
            .field("hint", &crate::redact::urls(&self.hint))
            .finish()
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::new(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::new(e.to_string())
    }
}

impl From<csv::Error> for Error {
    fn from(e: csv::Error) -> Self {
        Error::new(e.to_string())
    }
}

/// The library's result type.
pub type Result<T> = std::result::Result<T, Error>;
