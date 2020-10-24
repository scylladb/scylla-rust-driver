use anyhow::Result;

use crate::transport::session::Session;

use super::{prepared_statement::PreparedStatement, query::Query, Consistency, QueryParameters};

#[derive(Default, Debug, Clone)]
pub struct Builder {
    default_params: QueryParameters,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            default_params: Default::default(),
        }
    }

    pub fn build(&self) -> BuilderSession {
        BuilderSession {
            params: self.default_params.clone(),
        }
    }

    pub fn set_default_page_size(&mut self, page_size: i32) {
        assert!(page_size > 0, "page size must be larger than 0");
        self.default_params.page_size = Some(page_size);
    }

    pub fn set_no_paging_by_default(&mut self) {
        self.default_params.page_size = None;
    }

    pub fn set_default_consistency(&mut self, consistency: Consistency) {
        self.default_params.consistency = consistency;
    }
}

pub struct BuilderSession {
    params: QueryParameters,
}

impl BuilderSession {
    pub fn with_page_size(mut self, page_size: i32) -> Self {
        assert!(page_size > 0, "page size must be larger than 0");
        self.params.page_size = Some(page_size);
        self
    }

    pub fn with_no_paging(mut self) -> Self {
        self.params.page_size = None;
        self
    }

    pub fn with_consistency(mut self, consistency: Consistency) -> Self {
        self.params.consistency = consistency;
        self
    }

    pub async fn prepared(
        self,
        session: &Session,
        contents: impl AsRef<str>,
    ) -> Result<PreparedStatement> {
        session
            .prepare_with_params(contents.as_ref(), self.params)
            .await
    }

    pub fn query(self, contents: impl Into<String>) -> Query {
        Query::new(contents.into(), self.params)
    }
}
