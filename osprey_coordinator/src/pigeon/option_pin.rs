use pin_project::pin_project;

/// A pin-project compatible `Option`.
/// Copy of [`tonic::util::OptionPin`]
#[pin_project(project = OptionPinProj)]
pub(crate) enum OptionPin<T> {
    Some {
        #[pin]
        inner: T,
    },
    None,
}
