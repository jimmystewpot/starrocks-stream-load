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

fn find_case_insensitive(haystack: &str, needle: &str) -> Option<usize> {
    let h_bytes = haystack.as_bytes();
    let n_bytes = needle.as_bytes();
    if n_bytes.is_empty() || h_bytes.len() < n_bytes.len() {
        return None;
    }
    for i in 0..=(h_bytes.len() - n_bytes.len()) {
        let mut matches = true;
        for j in 0..n_bytes.len() {
            let b1 = h_bytes[i + j];
            let b2 = n_bytes[j];
            if !b1.eq_ignore_ascii_case(&b2) {
                matches = false;
                break;
            }
        }
        if matches {
            return Some(i);
        }
    }
    None
}

#[must_use]
pub fn redact_sensitive_info(input: &str) -> String {
    let mut output = input.to_string();

    // 1. Redact Basic Auth header tokens: "Basic <token>"
    // Find "Basic " (case-insensitive) and replace the subsequent base64 characters
    if let Some(idx) = find_case_insensitive(&output, "basic ") {
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
        while start_search < output.len() {
            if let Some(idx) = find_case_insensitive(&output[start_search..], word) {
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
            } else {
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

#[must_use]
pub fn sanitize_error_log(error_log: &str) -> String {
    if error_log.is_empty() || error_log.trim().is_empty() {
        return error_log.to_string();
    }

    let normalized = error_log.replace("\r\n", "\n").replace('\r', "\n");
    let lines = normalized.split('\n');
    let mut sanitized = String::new();

    for line in lines {
        if line.trim().is_empty() {
            continue;
        }

        let mut sanitized_line = line.to_string();

        // 1. Sanitize column values in all lines
        while let Some(start_idx) = sanitized_line.find("Value ''") {
            let rest = &sanitized_line[start_idx + 8..];
            if let Some(end_offset) = rest.find("''") {
                let end_idx = start_idx + 8 + end_offset + 2;
                sanitized_line.replace_range(start_idx..end_idx, "column value");
            } else {
                break;
            }
        }

        while let Some(start_idx) = sanitized_line.find("Value '") {
            let rest = &sanitized_line[start_idx + 7..];
            if let Some(end_offset) = rest.find('\'') {
                let end_idx = start_idx + 7 + end_offset + 1;
                sanitized_line.replace_range(start_idx..end_idx, "column value");
            } else {
                break;
            }
        }

        while let Some(start_idx) = sanitized_line.find("Value \"") {
            let rest = &sanitized_line[start_idx + 7..];
            if let Some(end_offset) = rest.find('"') {
                let end_idx = start_idx + 7 + end_offset + 1;
                sanitized_line.replace_range(start_idx..end_idx, "column value");
            } else {
                break;
            }
        }

        // 2. If line contains "Row:", remove the row data and everything after it
        if let Some(row_idx) = sanitized_line.find("Row:") {
            sanitized_line.truncate(row_idx);
        }

        if !sanitized_line.trim().is_empty() {
            sanitized.push_str(&sanitized_line);
            sanitized.push('\n');
        }
    }

    let result = sanitized.trim().to_string();
    if result.is_empty() {
        "Data validation errors detected. Row data has been redacted for security.".to_string()
    } else {
        result
    }
}

#[must_use]
pub fn try_get_error_log_url_from_txn_abort_reason(abort_reason: &str) -> Option<String> {
    let lower = abort_reason.to_lowercase();
    if let Some(idx) = lower.find("tracking url:") {
        let start_pos = idx + "tracking url:".len();
        let remaining = &abort_reason[start_pos..];

        // Skip leading whitespace
        let mut url_start = 0;
        let bytes = remaining.as_bytes();
        while url_start < bytes.len()
            && (bytes[url_start] == b' '
                || bytes[url_start] == b'\t'
                || bytes[url_start] == b'\r'
                || bytes[url_start] == b'\n')
        {
            url_start += 1;
        }

        if url_start < bytes.len() {
            let url_part = &remaining[url_start..];
            // Find end of URL (space, newline, or dot/comma followed by space/newline/end of string)
            let mut url_end = 0;
            let url_bytes = url_part.as_bytes();
            while url_end < url_bytes.len() {
                let c = url_bytes[url_end];
                if c == b' ' || c == b'\t' || c == b'\r' || c == b'\n' {
                    break;
                }
                if (c == b'.' || c == b',')
                    && (url_end + 1 == url_bytes.len()
                        || url_bytes[url_end + 1] == b' '
                        || url_bytes[url_end + 1] == b'\t'
                        || url_bytes[url_end + 1] == b'\r'
                        || url_bytes[url_end + 1] == b'\n')
                {
                    break;
                }
                url_end += 1;
            }
            let url = &url_part[..url_end];
            let url_lower = url.to_lowercase();
            if (url_lower.starts_with("http://") || url_lower.starts_with("https://"))
                && url_lower.contains("/api/_load_error_log?file=error_log_")
            {
                return Some(url.to_string());
            }
        }
    }
    None
}
