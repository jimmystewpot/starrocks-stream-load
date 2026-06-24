use std::fmt;

#[derive(Debug)]
pub enum Error {
    Network(reqwest::Error),
    StarRocksFailure {
        status: String,
        message: String,
        error_log_url: Option<String>,
    },
    Transaction(String),
    UrlParse(url::ParseError),
    Json(serde_json::Error),
    Io(std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Network(e) => Some(e),
            Self::UrlParse(e) => Some(e),
            Self::Json(e) => Some(e),
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Self::Network(err)
    }
}

impl From<url::ParseError> for Error {
    fn from(err: url::ParseError) -> Self {
        Self::UrlParse(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let raw_message = match self {
            Self::Network(e) => format!("Network failure occurred: {e}"),
            Self::StarRocksFailure {
                status,
                message,
                error_log_url,
            } => {
                format!(
                    "StarRocks load failed with status: {status}, message: {message}, error_log_url: {error_log_url:?}"
                )
            }
            Self::Transaction(msg) => format!("Transaction processing error: {msg}"),
            Self::UrlParse(e) => format!("URL parsing failure: {e}"),
            Self::Json(e) => format!("JSON processing error: {e}"),
            Self::Io(e) => format!("IO error: {e}"),
        };

        // Redact any sensitive information in the error message
        let redacted = redact_sensitive_info(&raw_message);
        write!(f, "{redacted}")
    }
}

#[must_use]
pub fn redact_sensitive_info(input: &str) -> String {
    let mut output = input.to_string();

    // 1. Redact Basic Auth header tokens: "Basic <token>"
    // Find "Basic " (case-insensitive) and replace the subsequent base64 characters
    if let Some(idx) = output.to_lowercase().find("basic ") {
        let start = idx + 6;
        let mut end = start;
        let bytes = output.as_bytes();
        while end < bytes.len()
            && (bytes[end].is_ascii_alphanumeric()
                || bytes[end] == b'+'
                || bytes[end] == b'/'
                || bytes[end] == b'=')
        {
            end += 1;
        }
        if end > start {
            output.replace_range(start..end, "***");
        }
    }

    // 2. Redact passwords in URL-like strings: e.g. "password=something" or "passwd=something"
    for word in &["password=", "passwd=", "pass="] {
        let mut start_search = 0;
        while let Some(idx) = output[start_search..].to_lowercase().find(word) {
            let actual_idx = start_search + idx;
            let val_start = actual_idx + word.len();
            let mut val_end = val_start;
            let bytes = output.as_bytes();
            while val_end < bytes.len()
                && bytes[val_end] != b'&'
                && bytes[val_end] != b' '
                && bytes[val_end] != b'\n'
                && bytes[val_end] != b','
            {
                val_end += 1;
            }
            if val_end > val_start {
                output.replace_range(val_start..val_end, "***");
            }
            start_search = val_start + 3; // move past the "***"
            if start_search >= output.len() {
                break;
            }
        }
    }

    // 3. Redact credentials in all inline URLs: e.g., "http://user:pass@host"
    let mut start_idx = 0;
    while let Some(proto_offset) = output[start_idx..].find("://") {
        let proto_idx = start_idx + proto_offset;
        let auth_start = proto_idx + 3;
        if let Some(at_offset) = output[auth_start..].find('@') {
            let actual_at_idx = auth_start + at_offset;
            // Ensure we don't scan past space/newline boundaries or path boundaries (e.g. '/')
            let chunk = &output[auth_start..actual_at_idx];
            if !chunk.contains(' ') && !chunk.contains('\n') && !chunk.contains('/') {
                if let Some(colon_offset) = chunk.find(':') {
                    let pass_start = auth_start + colon_offset + 1;
                    output.replace_range(pass_start..actual_at_idx, "***");
                    // Update start_idx past the modified region
                    start_idx = pass_start + 3; // length of "***"
                    continue;
                }
            }
        }
        start_idx = auth_start;
    }

    output
}
