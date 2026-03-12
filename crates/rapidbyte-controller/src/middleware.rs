//! Tower middleware for auth, tracing, and deadlines.
//!
//! The bearer auth interceptor is optional — when no tokens are configured,
//! all requests pass through.

use tonic::{Request, Status};

/// Bearer token authentication interceptor for tonic.
///
/// When `valid_tokens` is empty, auth is bypassed (permissive mode).
/// When tokens are configured, requests must include a valid
/// `Authorization: Bearer <token>` header.
#[derive(Debug, Clone)]
pub struct BearerAuthInterceptor {
    valid_tokens: Vec<String>,
}

impl BearerAuthInterceptor {
    pub fn new(tokens: Vec<String>) -> Self {
        Self {
            valid_tokens: tokens,
        }
    }

    /// Check a request for valid bearer token authentication.
    pub fn check<T>(&self, request: &Request<T>) -> Result<(), Status> {
        // No tokens configured = auth disabled
        if self.valid_tokens.is_empty() {
            return Ok(());
        }

        let metadata = request.metadata();
        let auth_header = metadata.get("authorization").and_then(|v| v.to_str().ok());

        match auth_header {
            Some(value) if value.starts_with("Bearer ") => {
                let token = &value[7..];
                if self.valid_tokens.iter().any(|t| t == token) {
                    Ok(())
                } else {
                    Err(Status::unauthenticated("Invalid bearer token"))
                }
            }
            Some(_) => Err(Status::unauthenticated(
                "Invalid authorization header format",
            )),
            None => Err(Status::unauthenticated("Missing authorization header")),
        }
    }
}

impl tonic::service::Interceptor for BearerAuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        self.check(&request)?;
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_with_token(token: &str) -> Request<()> {
        let mut req = Request::new(());
        req.metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        req
    }

    fn request_without_token() -> Request<()> {
        Request::new(())
    }

    #[test]
    fn valid_bearer_token_passes() {
        let interceptor = BearerAuthInterceptor::new(vec!["secret-token".into()]);
        let req = request_with_token("secret-token");
        assert!(interceptor.check(&req).is_ok());
    }

    #[test]
    fn invalid_bearer_token_is_rejected() {
        let interceptor = BearerAuthInterceptor::new(vec!["secret-token".into()]);
        let req = request_with_token("wrong-token");
        let err = interceptor.check(&req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn missing_token_when_auth_configured_is_rejected() {
        let interceptor = BearerAuthInterceptor::new(vec!["secret-token".into()]);
        let req = request_without_token();
        let err = interceptor.check(&req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn no_tokens_configured_passes_all_requests() {
        let interceptor = BearerAuthInterceptor::new(vec![]);
        let req = request_without_token();
        assert!(interceptor.check(&req).is_ok());
    }

    #[test]
    fn multiple_valid_tokens() {
        let interceptor = BearerAuthInterceptor::new(vec!["token-a".into(), "token-b".into()]);
        assert!(interceptor.check(&request_with_token("token-a")).is_ok());
        assert!(interceptor.check(&request_with_token("token-b")).is_ok());
        assert!(interceptor.check(&request_with_token("token-c")).is_err());
    }
}
