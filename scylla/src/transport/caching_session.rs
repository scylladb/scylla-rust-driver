use crate::frame::value::ValueList;
use crate::prepared_statement::PreparedStatement;
use crate::query::Query;
use crate::transport::errors::{DbError, QueryError};
use crate::transport::iterator::RowIterator;
use crate::{QueryResult, Session};
use bytes::Bytes;
use dashmap::DashMap;
use itertools::Either;

/// Provides auto caching while executing queries
pub struct CachingSession {
    pub session: Session,
    /// The prepared statement cache size
    /// If a prepared statement is added while the limit is reached, the oldest prepared statement
    /// is removed from the cache
    pub max_capacity: usize,
    pub cache: DashMap<String, PreparedStatement>,
}

impl CachingSession {
    pub fn from(session: Session, cache_size: usize) -> Self {
        Self {
            session,
            max_capacity: cache_size,
            cache: Default::default(),
        }
    }

    /// Does the same thing as [`Session::execute`] but uses the prepared statement cache
    pub async fn execute(
        &self,
        query: impl Into<Query>,
        values: &impl ValueList,
    ) -> Result<QueryResult, QueryError> {
        let query = query.into();
        let prepared = self.add_prepared_statement(&query).await?;
        let values = values.serialized()?;
        let result = self.session.execute(&prepared, values.clone()).await;

        match self.post_execute_prepared_statement(&query, result).await? {
            Either::Left(result) => Ok(result),
            Either::Right(new_prepared_statement) => {
                self.session.execute(&new_prepared_statement, values).await
            }
        }
    }

    /// Does the same thing as [`Session::execute_iter`] but uses the prepared statement cache
    pub async fn execute_iter(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
    ) -> Result<RowIterator, QueryError> {
        let query = query.into();
        let prepared = self.add_prepared_statement(&query).await?;
        let values = values.serialized()?;
        let result = self.session.execute_iter(prepared, values.clone()).await;

        match self.post_execute_prepared_statement(&query, result).await? {
            Either::Left(result) => Ok(result),
            Either::Right(new_prepared_statement) => {
                self.session
                    .execute_iter(new_prepared_statement, values)
                    .await
            }
        }
    }

    /// Does the same thing as [`Session::execute_paged`] but uses the prepared statement cache
    pub async fn execute_paged(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
        paging_state: Option<Bytes>,
    ) -> Result<QueryResult, QueryError> {
        let query = query.into();
        let prepared = self.add_prepared_statement(&query).await?;
        let values = values.serialized()?;
        let result = self
            .session
            .execute_paged(&prepared, values.clone(), paging_state.clone())
            .await;

        match self.post_execute_prepared_statement(&query, result).await? {
            Either::Left(result) => Ok(result),
            Either::Right(new_prepared_statement) => {
                self.session
                    .execute_paged(&new_prepared_statement, values, paging_state)
                    .await
            }
        }
    }

    /// Adds a prepared statement to the cache
    pub async fn add_prepared_statement(
        &self,
        query: impl Into<&Query>,
    ) -> Result<PreparedStatement, QueryError> {
        let query = query.into();

        if let Some(prepared) = self.cache.get(&query.contents) {
            // Clone, because else the value is mutably borrowed and the execute method gives a compile error
            Ok(prepared.clone())
        } else {
            let prepared = self.session.prepare(query.clone()).await?;

            if self.max_capacity == self.cache.len() {
                // Cache is full, remove the first entry
                // Don't hold a reference into the map (that's why the to_string() is called)
                // This is because the documentation of the remove fn tells us that it may deadlock
                // when holding some sort of reference into the map
                let query = self.cache.iter().next().map(|c| c.key().to_string());

                // Don't inline this: https://stackoverflow.com/questions/69873846/an-owned-value-is-still-references-somehow
                if let Some(q) = query {
                    self.cache.remove(&q);
                }
            }

            self.cache.insert(query.contents.clone(), prepared.clone());

            Ok(prepared)
        }
    }

    /// This method is called after a cached prepared statement
    /// It returns:
    ///     - a success result, nothing has to be done: [`Either::Left`]
    ///     - a new prepared statement in which the caller should retry the query: [`Either::Right`].
    ///     - [`QueryError`] when an error occurred
    async fn post_execute_prepared_statement<T>(
        &self,
        query: &Query,
        result: Result<T, QueryError>,
    ) -> Result<Either<T, PreparedStatement>, QueryError> {
        match result {
            Ok(qr) => Ok(Either::Left(qr)),
            Err(err) => {
                // Check if the 'Unprepare; error is thrown
                // In that case, re-prepare it and send it again
                // In all other cases, just return the error
                match err {
                    QueryError::DbError(db_error, message) => match db_error {
                        DbError::Unprepared { .. } => {
                            self.cache.remove(&query.contents);

                            let prepared = self.add_prepared_statement(query).await?;

                            Ok(Either::Right(prepared))
                        }
                        _ => Err(QueryError::DbError(db_error, message)),
                    },
                    _ => Err(err),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{CachingSession, Session, SessionBuilder};
    use futures::TryStreamExt;

    async fn new_for_test() -> Session {
        let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        let session = SessionBuilder::new()
            .known_node(uri)
            .build()
            .await
            .expect("Could not create session");
        let ks = crate::transport::session_test::unique_name();

        session
            .query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[])
            .await
            .expect("Could not create keyspace");

        session
            .query(
                format!(
                    "CREATE TABLE IF NOT EXISTS {}.test_table (a int primary key, b int)",
                    ks
                ),
                &[],
            )
            .await
            .expect("Could not create table");

        session
            .use_keyspace(ks, false)
            .await
            .expect("Could not set keyspace");

        session
    }

    async fn create_caching_session() -> CachingSession {
        let session = CachingSession::from(new_for_test().await, 2);

        // Add a row, this makes it easier to check if the caching works combined with the regular execute fn on Session
        session
            .execute("insert into test_table(a, b) values (1, 2)", &[])
            .await
            .unwrap();

        // Clear the cache because it now contains an insert
        assert_eq!(session.cache.len(), 1);

        session.cache.clear();

        session
    }

    /// Test that when the cache is full and a different query comes in, that query will be added
    /// to the cache and a random query is removed
    #[tokio::test]
    async fn test_full() {
        let session = create_caching_session().await;

        let first_query = "select * from test_table";
        let middle_query = "insert into test_table(a, b) values (?, ?)";
        let last_query = "update test_table set b = ? where a = 1";

        session
            .add_prepared_statement(&first_query.into())
            .await
            .unwrap();
        session
            .add_prepared_statement(&middle_query.into())
            .await
            .unwrap();
        session
            .add_prepared_statement(&last_query.into())
            .await
            .unwrap();

        assert_eq!(2, session.cache.len());

        // This query should be in the cache
        assert!(session.cache.get(last_query).is_some());

        // Either the first or middle query should be removed
        let first_query_removed = session.cache.get(first_query).is_none();
        let middle_query_removed = session.cache.get(middle_query).is_none();

        assert!(first_query_removed || middle_query_removed);
    }

    /// Checks that the same prepared statement is reused when executing the same query twice
    #[tokio::test]
    async fn test_execute_cached() {
        let session = create_caching_session().await;
        let result = session
            .execute("select * from test_table", &[])
            .await
            .unwrap();

        assert_eq!(1, session.cache.len());
        assert_eq!(1, result.rows.unwrap().len());

        let result = session
            .execute("select * from test_table", &[])
            .await
            .unwrap();

        assert_eq!(1, session.cache.len());
        assert_eq!(1, result.rows.unwrap().len());
    }

    /// Checks that caching works with execute_iter
    #[tokio::test]
    async fn test_execute_iter_cached() {
        let session = create_caching_session().await;

        assert!(session.cache.is_empty());

        let iter = session
            .execute_iter("select * from test_table", &[])
            .await
            .unwrap();

        let rows = iter.try_collect::<Vec<_>>().await.unwrap().len();

        assert_eq!(1, rows);
        assert_eq!(1, session.cache.len());
    }

    /// Checks that caching works with execute_paged
    #[tokio::test]
    async fn test_execute_paged_cached() {
        let session = create_caching_session().await;

        assert!(session.cache.is_empty());

        let result = session
            .execute_paged("select * from test_table", &[], None)
            .await
            .unwrap();

        assert_eq!(1, session.cache.len());
        assert_eq!(1, result.rows.unwrap().len());
    }
}
