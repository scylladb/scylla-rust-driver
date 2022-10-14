use std::{fmt::Debug, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use scylla_cql::{frame::types::SerialConsistency, Consistency};

use crate::{
    load_balancing::LoadBalancingPolicy, retry_policy::RetryPolicy,
    speculative_execution::SpeculativeExecutionPolicy,
};

use super::session;

/// `ExecutionProfileBuilder` is used to create new `ExecutionProfile`s
/// # Example
///
/// ```
/// # use scylla::transport::{ExecutionProfile, retry_policy::FallthroughRetryPolicy};
/// # use scylla::statement::Consistency;
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let profile: ExecutionProfile = ExecutionProfile::builder()
///     .consistency(Consistency::Three) // as this is the number we shall count to
///     .retry_policy(Box::new(FallthroughRetryPolicy::new()))
///     .build();
/// # Ok(())
/// # }
/// ```
pub struct ExecutionProfileBuilder {
    request_timeout: Option<Option<Duration>>,
    consistency: Option<Consistency>,
    serial_consistency: Option<Option<SerialConsistency>>,
    load_balancing_policy: Option<Arc<dyn LoadBalancingPolicy>>,
    retry_policy: Option<Box<dyn RetryPolicy>>,
    speculative_execution_policy: Option<Option<Arc<dyn SpeculativeExecutionPolicy>>>,
}

impl ExecutionProfileBuilder {
    /// Changes client-side timeout.
    /// The default is 30 seconds.
    ///
    /// # Example
    /// ```
    /// # use scylla::transport::ExecutionProfile;
    /// # use std::time::Duration;
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let profile: ExecutionProfile = ExecutionProfile::builder()
    ///     .request_timeout(Some(Duration::from_secs(5)))
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn request_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Specify a default consistency to be used for queries.
    /// It's possible to override it by explicitly setting a consistency on the chosen query.
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.consistency = Some(consistency);
        self
    }

    /// Specify a default serial consistency to be used for queries.
    /// It's possible to override it by explicitly setting a serial consistency
    /// on the chosen query.
    pub fn serial_consistency(mut self, serial_consistency: Option<SerialConsistency>) -> Self {
        self.serial_consistency = Some(serial_consistency);
        self
    }

    /// Sets the load balancing policy.
    /// The default is Token-aware Round-robin.
    ///
    /// # Example
    /// ```
    /// # use scylla::transport::ExecutionProfile;
    /// # use scylla::transport::load_balancing::RoundRobinPolicy;
    /// # use std::sync::Arc;
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let profile: ExecutionProfile = ExecutionProfile::builder()
    ///     .load_balancing_policy(Arc::new(RoundRobinPolicy::new()))
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn load_balancing_policy(
        mut self,
        load_balancing_policy: Arc<dyn LoadBalancingPolicy>,
    ) -> Self {
        self.load_balancing_policy = Some(load_balancing_policy);
        self
    }

    /// Sets the [`RetryPolicy`] to use by default on queries.
    /// The default is [DefaultRetryPolicy](crate::transport::retry_policy::DefaultRetryPolicy).
    /// It is possible to implement a custom retry policy by implementing the trait [`RetryPolicy`].
    ///
    /// # Example
    /// ```
    /// use scylla::transport::retry_policy::DefaultRetryPolicy;
    /// # use scylla::transport::ExecutionProfile;
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let profile: ExecutionProfile = ExecutionProfile::builder()
    ///     .retry_policy(Box::new(DefaultRetryPolicy::new()))
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn retry_policy(mut self, retry_policy: Box<dyn RetryPolicy>) -> Self {
        self.retry_policy = Some(retry_policy);
        self
    }

    /// Sets the speculative execution policy.
    /// The default is None.
    /// # Example
    /// ```
    /// # extern crate scylla;
    /// # use std::error::Error;
    /// # fn check_only_compiles() -> Result<(), Box<dyn Error>> {
    /// use std::{sync::Arc, time::Duration};
    /// use scylla::{
    ///     transport::ExecutionProfile,
    ///     transport::speculative_execution::SimpleSpeculativeExecutionPolicy,
    /// };
    ///
    /// let policy = SimpleSpeculativeExecutionPolicy {
    ///     max_retry_count: 3,
    ///     retry_interval: Duration::from_millis(100),
    /// };
    ///
    /// let profile: ExecutionProfile = ExecutionProfile::builder()
    ///     .speculative_execution_policy(Some(Arc::new(policy)))
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn speculative_execution_policy(
        mut self,
        speculative_execution_policy: Option<Arc<dyn SpeculativeExecutionPolicy>>,
    ) -> Self {
        self.speculative_execution_policy = Some(speculative_execution_policy);
        self
    }

    /// Builds the ExecutionProfile after setting all the options.
    ///
    /// # Example
    /// ```
    /// use scylla::transport::retry_policy::DefaultRetryPolicy;
    /// # use scylla::transport::ExecutionProfile;
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let profile: ExecutionProfile = ExecutionProfile::builder()
    ///     .retry_policy(Box::new(DefaultRetryPolicy::new()))
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn build(self) -> ExecutionProfile {
        use session::defaults;
        ExecutionProfile(Arc::new(ExecutionProfileInner {
            request_timeout: self
                .request_timeout
                .unwrap_or_else(defaults::request_timeout),
            consistency: self.consistency.unwrap_or_else(defaults::consistency),
            serial_consistency: self
                .serial_consistency
                .unwrap_or_else(defaults::serial_consistency),
            load_balancing_policy: self
                .load_balancing_policy
                .unwrap_or_else(defaults::load_balancing_policy),
            retry_policy: self.retry_policy.unwrap_or_else(defaults::retry_policy),
            speculative_execution_policy: self
                .speculative_execution_policy
                .unwrap_or_else(defaults::speculative_execution_policy),
        }))
    }
}

/// A profile that groups configurable options regarding query execution.
///
/// Execution profile is immutable as such, but the driver implements double indirection of form:
/// query/Session -> ExecutionProfileHandle -> ExecutionProfile
/// which enables on-fly changing the actual profile associated with all entities (query/Session)
/// by the same handle.
#[derive(Debug, Clone)]
pub struct ExecutionProfile(pub(crate) Arc<ExecutionProfileInner>);

#[derive(Debug)]
pub(crate) struct ExecutionProfileInner {
    pub(crate) request_timeout: Option<Duration>,

    pub(crate) consistency: Consistency,
    pub(crate) serial_consistency: Option<SerialConsistency>,

    pub(crate) load_balancing_policy: Arc<dyn LoadBalancingPolicy>,
    pub(crate) retry_policy: Box<dyn RetryPolicy>,
    pub(crate) speculative_execution_policy: Option<Arc<dyn SpeculativeExecutionPolicy>>,
}

impl ExecutionProfile {
    pub(crate) fn new_from_inner(inner: ExecutionProfileInner) -> Self {
        Self(Arc::new(inner))
    }

    /// Creates a blank builder that can be used to construct new ExecutionProfile.
    pub fn builder() -> ExecutionProfileBuilder {
        ExecutionProfileBuilder {
            request_timeout: None,
            consistency: None,
            serial_consistency: None,
            load_balancing_policy: None,
            retry_policy: None,
            speculative_execution_policy: None,
        }
    }

    /// Creates a builder having all options set to the same as set in this ExecutionProfile.
    pub fn to_builder(&self) -> ExecutionProfileBuilder {
        ExecutionProfileBuilder {
            request_timeout: Some(self.0.request_timeout),
            consistency: Some(self.0.consistency),
            serial_consistency: Some(self.0.serial_consistency),
            load_balancing_policy: Some(self.0.load_balancing_policy.clone()),
            retry_policy: Some(self.0.retry_policy.clone()),
            speculative_execution_policy: Some(self.0.speculative_execution_policy.clone()),
        }
    }

    /// Returns a new handle to this ExecutionProfile.
    pub fn into_handle(self) -> ExecutionProfileHandle {
        ExecutionProfileHandle(Arc::new((ArcSwap::new(self.0), None)))
    }

    /// Returns a new handle to this ExecutionProfile, tagging the handle with provided label.
    /// The tag, as its name suggests, is only useful for debugging purposes, while being confused
    /// about which statement/session is assigned which handle. Identifying handles with tags
    /// could then help.
    pub fn into_handle_with_label(self, label: String) -> ExecutionProfileHandle {
        ExecutionProfileHandle(Arc::new((ArcSwap::new(self.0), Some(label))))
    }
}

/// A handle that points to an ExecutionProfile.
///
/// Its goal is to enable remapping all associated entities (query/Session)
/// to another execution profile at once.
/// Note: Cloned handles initially point to the same Arc'ed execution profile.
/// However, as the mapping has yet another level of indirection - through
/// `Arc<ArcSwap>` - remapping one of them affects all the others, as under the hood
/// it is done by replacing the Arc held by the ArcSwap, which is shared
/// by all cloned handles.
/// The optional String is just for debug purposes. Its purpose is described
/// in [ExecutionProfile::into_handle_with_label].
#[derive(Debug, Clone)]
pub struct ExecutionProfileHandle(Arc<(ArcSwap<ExecutionProfileInner>, Option<String>)>);

impl ExecutionProfileHandle {
    pub(crate) fn access(&self) -> Arc<ExecutionProfileInner> {
        self.0 .0.load_full()
    }

    /// Makes the handle point to a new execution profile.
    /// All entities (queries/Session) holding this handle will reflect the change.
    pub fn map_to_another_profile(&mut self, profile: ExecutionProfile) {
        self.0 .0.store(profile.0)
    }
}
