use std::fmt;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ServerContext {
    id: Arc<str>,
}

impl ServerContext {
    pub(crate) fn new(id: String) -> Self {
        Self { id: id.into() }
    }

    #[must_use]
    pub(crate) fn id(&self) -> &str {
        &self.id
    }
}

impl fmt::Display for ServerContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.id)
    }
}
