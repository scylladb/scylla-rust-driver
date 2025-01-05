//! `ExecutionProfile` is a grouping of configurable options regarding query execution.
//!
//! Profiles can be created to represent different workloads, which thanks to them
//! can be run conveniently on a single session.
//!
//! There are two classes of objects related to execution profiles: `ExecutionProfile` and `ExecutionProfileHandle`.
//! The former is simply an immutable set of the settings. The latter is a handle that at particular moment points
//! to some `ExecutionProfile` (but during its lifetime, it can change the profile it points at).
//! Handles are assigned to `Sessions` and `Statements`.
//! At any moment, handles point to another `ExecutionProfile`. This allows convenient switching between workloads
//! for all `Sessions` and/or `Statements` that, for instance, share common characteristics.
//!
//! ### Example
//! To create an `ExecutionProfile` and attach it as default for `Session`:
//! ```
//! # extern crate scylla;
//! # use std::error::Error;
//! # async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
//! use scylla::client::session::Session;
//! use scylla::client::session_builder::SessionBuilder;
//! use scylla::statement::Consistency;
//! use scylla::client::execution_profile::ExecutionProfile;
//!
//! let profile = ExecutionProfile::builder()
//!     .consistency(Consistency::LocalOne)
//!     .request_timeout(None) // no request timeout
//!     .build();
//!
//! let handle = profile.into_handle();
//!
//! let session: Session = SessionBuilder::new()
//!     .known_node("127.0.0.1:9042")
//!     .default_execution_profile_handle(handle)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Example
//! To create an `ExecutionProfile` and attach it to a `Query`:
//! ```
//! # extern crate scylla;
//! # use std::error::Error;
//! # async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
//! use scylla::query::Query;
//! use scylla::statement::Consistency;
//! use scylla::client::execution_profile::ExecutionProfile;
//! use std::time::Duration;
//!
//! let profile = ExecutionProfile::builder()
//!     .consistency(Consistency::All)
//!     .request_timeout(Some(Duration::from_secs(30)))
//!     .build();
//!
//! let handle = profile.into_handle();
//!
//! let mut query1 = Query::from("SELECT * FROM ks.table");
//! query1.set_execution_profile_handle(Some(handle.clone()));
//!
//! let mut query2 = Query::from("SELECT pk FROM ks.table WHERE pk = ?");
//! query2.set_execution_profile_handle(Some(handle));
//! # Ok(())
//! # }
//! ```
//!
//! ### Example
//! To create an `ExecutionProfile` with config options defaulting
//! to those set on another profile:
//! ```
//! # extern crate scylla;
//! # use std::error::Error;
//! # async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
//! use scylla::statement::Consistency;
//! use scylla::client::execution_profile::ExecutionProfile;
//! use std::time::Duration;
//!
//! let base_profile = ExecutionProfile::builder()
//!     .request_timeout(Some(Duration::from_secs(30)))
//!     .build();
//!
//! let profile = base_profile.to_builder()
//!     .consistency(Consistency::All)
//!     .build();
//
//! # Ok(())
//! # }
//! ```
//!
//! `ExecutionProfileHandle`s can be remapped to another `ExecutionProfile`, and the change affects all sessions and statements that have been assigned that handle. This enables quick workload switches.
//!
//! Example mapping:
//! * session1 -> handle1 -> profile1
//! * statement1 -> handle1 -> profile1
//! * statement2 -> handle2 -> profile2
//!
//! We can now remap handle2 to profile1, so that the mapping for statement2 becomes as follows:
//! * statement2 -> handle2 -> profile1
//!
//! We can also change statement1's handle to handle2, and remap handle1 to profile2, yielding:
//! * session1 -> handle1 -> profile2
//! * statement1 -> handle2 -> profile1
//! * statement2 -> handle2 -> profile1
//!
//! As you can see, profiles are a powerful and convenient way to define and modify your workloads.
//!
//! ### Example
//! Below, the remaps described above are followed in code.
//! ```
//! # extern crate scylla;
//! # use std::error::Error;
//! # async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
//! use scylla::client::session::Session;
//! use scylla::client::session_builder::SessionBuilder;
//! use scylla::query::Query;
//! use scylla::statement::Consistency;
//! use scylla::client::execution_profile::ExecutionProfile;
//!
//! let profile1 = ExecutionProfile::builder()
//!     .consistency(Consistency::One)
//!     .build();
//!
//! let profile2 = ExecutionProfile::builder()
//!     .consistency(Consistency::Two)
//!     .build();
//!
//! let mut handle1 = profile1.clone().into_handle();
//! let mut handle2 = profile2.clone().into_handle();
//!
//! let session: Session = SessionBuilder::new()
//!     .known_node("127.0.0.1:9042")
//!     .default_execution_profile_handle(handle1.clone())
//!     .build()
//!     .await?;
//!
//! let mut query1 = Query::from("SELECT * FROM ks.table");
//! let mut query2 = Query::from("SELECT pk FROM ks.table WHERE pk = ?");
//!
//! query1.set_execution_profile_handle(Some(handle1.clone()));
//! query2.set_execution_profile_handle(Some(handle2.clone()));
//!
//! // session1 -> handle1 -> profile1
//! //   query1 -> handle1 -> profile1
//! //   query2 -> handle2 -> profile2
//!
//! // We can now remap handle2 to profile1:
//! handle2.map_to_another_profile(profile1);
//! // ...so that the mapping for query2 becomes as follows:
//! // query2 -> handle2 -> profile1
//!
//! // We can also change query1's handle to handle2:
//! query1.set_execution_profile_handle(Some(handle2.clone()));
//! // ...and remap handle1 to profile2:
//! handle1.map_to_another_profile(profile2);
//! // ...yielding:
//! // session1 -> handle1 -> profile2
//! //   query1 -> handle2 -> profile1
//! //   query2 -> handle2 -> profile1
//!
//! # Ok(())
//! # }
//! ```
//!

use std::{fmt::Debug, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use scylla_cql::{frame::types::SerialConsistency, Consistency};

use crate::load_balancing::LoadBalancingPolicy;
use crate::policies::retry::RetryPolicy;
use crate::policies::speculative_execution::SpeculativeExecutionPolicy;

pub(crate) mod defaults {
    use super::ExecutionProfileInner;
    use crate::load_balancing::{self, LoadBalancingPolicy};
    use crate::policies::retry::{DefaultRetryPolicy, RetryPolicy};
    use crate::policies::speculative_execution::SpeculativeExecutionPolicy;
    use scylla_cql::frame::types::SerialConsistency;
    use scylla_cql::Consistency;
    use std::sync::Arc;
    use std::time::Duration;
    pub(crate) fn consistency() -> Consistency {
        Consistency::LocalQuorum
    }
    pub(crate) fn serial_consistency() -> Option<SerialConsistency> {
        Some(SerialConsistency::LocalSerial)
    }
    pub(crate) fn request_timeout() -> Option<Duration> {
        Some(Duration::from_secs(30))
    }
    pub(crate) fn load_balancing_policy() -> Arc<dyn LoadBalancingPolicy> {
        Arc::new(load_balancing::DefaultPolicy::default())
    }
    pub(crate) fn retry_policy() -> Arc<dyn RetryPolicy> {
        Arc::new(DefaultRetryPolicy::new())
    }
    pub(crate) fn speculative_execution_policy() -> Option<Arc<dyn SpeculativeExecutionPolicy>> {
        None
    }

    impl Default for ExecutionProfileInner {
        fn default() -> Self {
            Self {
                request_timeout: request_timeout(),
                consistency: consistency(),
                serial_consistency: serial_consistency(),
                load_balancing_policy: load_balancing_policy(),
                retry_policy: retry_policy(),
                speculative_execution_policy: speculative_execution_policy(),
            }
        }
    }
}

/// `ExecutionProfileBuilder` is used to create new `ExecutionProfile`s
/// # Example
///
/// ```
/// # use scylla::client::execution_profile::ExecutionProfile;
/// # use scylla::policies::retry::FallthroughRetryPolicy;
/// # use scylla::statement::Consistency;
/// # use std::sync::Arc;
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let profile: ExecutionProfile = ExecutionProfile::builder()
///     .consistency(Consistency::Three) // as this is the number we shall count to
///     .retry_policy(Arc::new(FallthroughRetryPolicy::new()))
///     .build();
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ExecutionProfileBuilder {
    request_timeout: Option<Option<Duration>>,
    consistency: Option<Consistency>,
    serial_consistency: Option<Option<SerialConsistency>>,
    load_balancing_policy: Option<Arc<dyn LoadBalancingPolicy>>,
    retry_policy: Option<Arc<dyn RetryPolicy>>,
    speculative_execution_policy: Option<Option<Arc<dyn SpeculativeExecutionPolicy>>>,
}

impl ExecutionProfileBuilder {
    /// Changes client-side timeout.
    /// The default is 30 seconds.
    ///
    /// # Example
    /// ```
    /// # use scylla::client::execution_profile::ExecutionProfile;
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
    /// The default is DefaultPolicy.
    ///
    /// # Example
    /// ```
    /// # use scylla::client::execution_profile::ExecutionProfile;
    /// # use scylla::policies::load_balancing::DefaultPolicy;
    /// # use std::sync::Arc;
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let profile: ExecutionProfile = ExecutionProfile::builder()
    ///     .load_balancing_policy(Arc::new(DefaultPolicy::default()))
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
    /// The default is [DefaultRetryPolicy](crate::policies::retry::DefaultRetryPolicy).
    /// It is possible to implement a custom retry policy by implementing the trait [`RetryPolicy`].
    ///
    /// # Example
    /// ```
    /// # use scylla::client::execution_profile::ExecutionProfile;
    /// # use scylla::policies::retry::DefaultRetryPolicy;
    /// # use std::sync::Arc;
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let profile: ExecutionProfile = ExecutionProfile::builder()
    ///     .retry_policy(Arc::new(DefaultRetryPolicy::new()))
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn retry_policy(mut self, retry_policy: Arc<dyn RetryPolicy>) -> Self {
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
    ///     client::execution_profile::ExecutionProfile,
    ///     policies::speculative_execution::SimpleSpeculativeExecutionPolicy,
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
    /// # use scylla::client::execution_profile::ExecutionProfile;
    /// # use scylla::policies::retry::DefaultRetryPolicy;
    /// # use std::sync::Arc;
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let profile: ExecutionProfile = ExecutionProfile::builder()
    ///     .retry_policy(Arc::new(DefaultRetryPolicy::new()))
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn build(self) -> ExecutionProfile {
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

impl Default for ExecutionProfileBuilder {
    fn default() -> Self {
        ExecutionProfile::builder()
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
    pub(crate) retry_policy: Arc<dyn RetryPolicy>,
    pub(crate) speculative_execution_policy: Option<Arc<dyn SpeculativeExecutionPolicy>>,
}

impl ExecutionProfileInner {
    /// Creates a builder having all options set to the same as set in this ExecutionProfileInner.
    pub(crate) fn to_builder(&self) -> ExecutionProfileBuilder {
        ExecutionProfileBuilder {
            request_timeout: Some(self.request_timeout),
            consistency: Some(self.consistency),
            serial_consistency: Some(self.serial_consistency),
            load_balancing_policy: Some(self.load_balancing_policy.clone()),
            retry_policy: Some(self.retry_policy.clone()),
            speculative_execution_policy: Some(self.speculative_execution_policy.clone()),
        }
    }
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
        self.0.to_builder()
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

    /// Gets client timeout associated with this profile.
    pub fn get_request_timeout(&self) -> Option<Duration> {
        self.0.request_timeout
    }

    /// Gets consistency associated with this profile.
    pub fn get_consistency(&self) -> Consistency {
        self.0.consistency
    }

    /// Gets serial consistency (if set) associated with this profile.
    pub fn get_serial_consistency(&self) -> Option<SerialConsistency> {
        self.0.serial_consistency
    }

    /// Gets load balancing policy associated with this profile.
    pub fn get_load_balancing_policy(&self) -> &Arc<dyn LoadBalancingPolicy> {
        &self.0.load_balancing_policy
    }

    /// Gets retry policy associated with this profile.
    pub fn get_retry_policy(&self) -> &Arc<dyn RetryPolicy> {
        &self.0.retry_policy
    }

    /// Gets speculative execution policy associated with this profile.
    pub fn get_speculative_execution_policy(&self) -> Option<&Arc<dyn SpeculativeExecutionPolicy>> {
        self.0.speculative_execution_policy.as_ref()
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

    /// Creates a builder having all options set to the same as set in the ExecutionProfile pointed by this handle.
    pub fn pointee_to_builder(&self) -> ExecutionProfileBuilder {
        self.0 .0.load().to_builder()
    }

    /// Returns execution profile pointed by this handle.
    pub fn to_profile(&self) -> ExecutionProfile {
        ExecutionProfile(self.access())
    }

    /// Makes the handle point to a new execution profile.
    /// All entities (queries/Session) holding this handle will reflect the change.
    pub fn map_to_another_profile(&mut self, profile: ExecutionProfile) {
        self.0 .0.store(profile.0)
    }
}
