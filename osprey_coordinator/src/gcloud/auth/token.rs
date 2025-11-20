use std::time::Instant;

use serde::{self, Deserialize};

/// Represents an auth token for oauth
#[derive(Eq, PartialEq, Deserialize, Debug, Clone)]
pub struct Token {
    access_token: String,
    /// How long (in seconds) this token is valid.
    #[serde(rename = "expires_in")]
    pub expires_in_secs: u64,
    pub token_type: String,

    /// Timestamp of when we created this instance, and when `expires_in` takes effect.
    #[serde(skip, default = "Instant::now")]
    pub created_at: Instant,
}

/// Manually implement this so we don't print the `access_token`
impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Token: (type: {}, expires_in: {}s, alive_for: {}s)",
            self.token_type,
            self.expires_in_secs,
            self.created_at.elapsed().as_secs(),
        )
    }
}

impl Token {
    pub fn new(
        access_token: String,
        expires_in_secs: u64,
        token_type: String,
        created_at: Instant,
    ) -> Self {
        Self {
            access_token,
            expires_in_secs,
            token_type,
            created_at,
        }
    }

    pub fn from_goauth(token: goauth::auth::Token, created_at: Instant) -> Self {
        Self {
            access_token: token.access_token().to_string(),
            expires_in_secs: token.expires_in() as u64,
            token_type: token.token_type().to_string(),
            created_at,
        }
    }

    pub fn is_expired(&self) -> bool {
        let secs_since_created = self.created_at.elapsed().as_secs();
        secs_since_created > self.expires_in_secs
    }

    /// Returns an active duration of how much time is left before the token expires.
    /// `0` is returned if expired.
    pub fn ttl_secs(&self) -> u64 {
        let secs_since_created = self.created_at.elapsed().as_secs();

        self.expires_in_secs.saturating_sub(secs_since_created)
    }

    /// Returns a header value representation of the token
    pub fn to_header_rep(&self) -> String {
        format!("{} {}", self.token_type, self.access_token)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_token_expired() {
        let token = Token {
            access_token: "test.Sample".to_string(),
            expires_in_secs: 3,
            token_type: "Bearer".to_string(),
            created_at: Instant::now() - Duration::from_secs(4),
        };

        assert!(token.is_expired());
        assert_eq!(token.ttl_secs(), 0, "time remaining should be 0");
    }

    #[test]
    fn test_token_not_expired() {
        let token = Token {
            access_token: "test.Sample".to_string(),
            expires_in_secs: 30,
            token_type: "Bearer".to_string(),
            created_at: Instant::now() - Duration::from_secs(1),
        };

        assert!(!token.is_expired());
        let ttl = token.ttl_secs();
        assert!(ttl >= 20 && ttl <= 29, "time remaining should be < expires");
    }
}
