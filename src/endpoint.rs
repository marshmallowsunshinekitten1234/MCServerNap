use std::fmt;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Endpoint {
    host: Arc<str>,
    port: u16,
}

impl Endpoint {
    #[must_use]
    pub fn new(host: impl Into<Arc<str>>, port: u16) -> Self {
        let host = host.into();
        if let Some(unbracketed) = host
            .strip_prefix('[')
            .and_then(|value| value.strip_suffix(']'))
        {
            return Self {
                host: Arc::from(unbracketed),
                port,
            };
        }
        Self { host, port }
    }

    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(formatter, "[{}]:{}", self.host, self.port)
        } else {
            write!(formatter, "{}:{}", self.host, self.port)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_ipv6_with_brackets_while_storing_an_unbracketed_host() {
        let ipv4 = Endpoint::new("127.0.0.1", 25_575);
        let hostname = Endpoint::new("localhost", 25_575);
        let ipv6 = Endpoint::new("::1", 25_575);
        let bracketed_ipv6 = Endpoint::new("[::1]", 25_575);

        assert_eq!(ipv4.to_string(), "127.0.0.1:25575");
        assert_eq!(hostname.to_string(), "localhost:25575");
        assert_eq!(ipv6.to_string(), "[::1]:25575");
        assert_eq!(bracketed_ipv6, ipv6);
        assert_eq!(ipv6.host(), "::1");
        assert_eq!(ipv6.port(), 25_575);
    }
}
