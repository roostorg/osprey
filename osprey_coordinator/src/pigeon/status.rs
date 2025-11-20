//! Copy of private methods from
//! https://github.com/hyperium/tonic/blob/v0.6.2/tonic/src/status.rs

use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use percent_encoding::{percent_encode, AsciiSet, CONTROLS};
use std::{borrow::Cow, error::Error, fmt};
use tonic::{metadata::MetadataMap, Code, Status};
use tracing::debug;

use super::grpc_timeout::TimeoutExpired;

const ENCODING_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'<')
    .add(b'>')
    .add(b'`')
    .add(b'?')
    .add(b'{')
    .add(b'}');

pub(crate) const GRPC_STATUS_HEADER_CODE: &str = "grpc-status";
const GRPC_STATUS_MESSAGE_HEADER: &str = "grpc-message";
const GRPC_STATUS_DETAILS_HEADER: &str = "grpc-status-details-bin";

pub(crate) fn try_status_from_error(
    err: Box<dyn Error + Send + Sync + 'static>,
) -> Result<Status, Box<dyn Error + Send + Sync + 'static>> {
    let err = match err.downcast::<Status>() {
        Ok(status) => {
            return Ok(*status);
        }
        Err(err) => err,
    };

    let err = match err.downcast::<h2::Error>() {
        Ok(h2) => {
            return Ok(from_h2_error(&*h2));
        }
        Err(err) => err,
    };

    if let Some(status) = find_status_in_source_chain(&*err) {
        // Difference from upstream:
        // can't set source because it is private to tonic.
        return Ok(status);
    }

    Err(err)
}

fn find_status_in_source_chain(err: &(dyn Error + 'static)) -> Option<Status> {
    let mut source = Some(err);

    while let Some(err) = source {
        if let Some(status) = err.downcast_ref::<Status>() {
            return Some(Status::with_details_and_metadata(
                status.code(),
                status.message().to_string(),
                Bytes::copy_from_slice(status.details()),
                status.metadata().clone(),
            ));
        }

        if let Some(timeout) = err.downcast_ref::<TimeoutExpired>() {
            return Some(Status::cancelled(timeout.to_string()));
        }

        if let Some(hyper) = err
            .downcast_ref::<hyper::Error>()
            .and_then(from_hyper_error)
        {
            return Some(hyper);
        }

        source = err.source();
    }

    None
}

fn from_h2_error(err: &h2::Error) -> Status {
    // See https://github.com/grpc/grpc/blob/3977c30/doc/PROTOCOL-HTTP2.md#errors
    let code = match err.reason() {
        Some(h2::Reason::NO_ERROR)
        | Some(h2::Reason::PROTOCOL_ERROR)
        | Some(h2::Reason::INTERNAL_ERROR)
        | Some(h2::Reason::FLOW_CONTROL_ERROR)
        | Some(h2::Reason::SETTINGS_TIMEOUT)
        | Some(h2::Reason::COMPRESSION_ERROR)
        | Some(h2::Reason::CONNECT_ERROR) => Code::Internal,
        Some(h2::Reason::REFUSED_STREAM) => Code::Unavailable,
        Some(h2::Reason::CANCEL) => Code::Cancelled,
        Some(h2::Reason::ENHANCE_YOUR_CALM) => Code::ResourceExhausted,
        Some(h2::Reason::INADEQUATE_SECURITY) => Code::PermissionDenied,

        _ => Code::Unknown,
    };

    // Difference from upstream:
    // can't set source because it is private to tonic.
    Status::new(code, format!("h2 protocol error: {}", err))
}

fn from_hyper_error(err: &hyper::Error) -> Option<Status> {
    if err.is_timeout() || err.is_connect() {
        return Some(Status::unavailable(err.to_string()));
    }
    None
}

pub(crate) fn add_status_header(status: &Status, header_map: &mut HeaderMap) -> Result<(), Status> {
    header_map.extend(metadata_into_sanitized_headers(status.metadata().clone()));

    header_map.insert(GRPC_STATUS_HEADER_CODE, code_to_header_value(status.code()));

    if !status.message().is_empty() {
        let to_write = Bytes::copy_from_slice(
            Cow::from(percent_encode(status.message().as_bytes(), ENCODING_SET)).as_bytes(),
        );

        header_map.insert(
            GRPC_STATUS_MESSAGE_HEADER,
            HeaderValue::from_maybe_shared(to_write).map_err(invalid_header_value_byte)?,
        );
    }

    if !status.details().is_empty() {
        use base64::engine::{general_purpose::STANDARD_NO_PAD, Engine};
        let details = STANDARD_NO_PAD.encode(status.details());

        header_map.insert(
            GRPC_STATUS_DETAILS_HEADER,
            HeaderValue::from_maybe_shared(details).map_err(invalid_header_value_byte)?,
        );
    }

    Ok(())
}

fn code_to_header_value(code: Code) -> HeaderValue {
    match code {
        Code::Ok => HeaderValue::from_static("0"),
        Code::Cancelled => HeaderValue::from_static("1"),
        Code::Unknown => HeaderValue::from_static("2"),
        Code::InvalidArgument => HeaderValue::from_static("3"),
        Code::DeadlineExceeded => HeaderValue::from_static("4"),
        Code::NotFound => HeaderValue::from_static("5"),
        Code::AlreadyExists => HeaderValue::from_static("6"),
        Code::PermissionDenied => HeaderValue::from_static("7"),
        Code::ResourceExhausted => HeaderValue::from_static("8"),
        Code::FailedPrecondition => HeaderValue::from_static("9"),
        Code::Aborted => HeaderValue::from_static("10"),
        Code::OutOfRange => HeaderValue::from_static("11"),
        Code::Unimplemented => HeaderValue::from_static("12"),
        Code::Internal => HeaderValue::from_static("13"),
        Code::Unavailable => HeaderValue::from_static("14"),
        Code::DataLoss => HeaderValue::from_static("15"),
        Code::Unauthenticated => HeaderValue::from_static("16"),
    }
}

const GRPC_RESERVED_HEADERS: [&str; 6] = [
    "te",
    "user-agent",
    "content-type",
    "grpc-message",
    "grpc-message-type",
    "grpc-status",
];

fn metadata_into_sanitized_headers(mut metadata: MetadataMap) -> http::HeaderMap {
    for r in &GRPC_RESERVED_HEADERS {
        metadata.remove(*r);
    }
    metadata.into_headers()
}

fn invalid_header_value_byte<Error: fmt::Display>(err: Error) -> Status {
    debug!("Invalid header: {}", err);
    Status::new(
        Code::Internal,
        "Couldn't serialize non-text grpc status header".to_string(),
    )
}
