# Speculative execution

Speculative query execution is an optimization technique where a driver
pre-emptively starts a second execution of a query against another node,
before the first node has replied.

There are multiple speculative execution strategies that the driver can use.
Speculative execution can be configured for the whole whole `Session` during
its creation.

Available speculative execution strategies:
* [Simple](simple.md)
* [Latency Percentile](percentile.md)

Speculative execution is not enabled by default, and currently only
non-iter session methods use it.

### When does speculative execution actually fire?

Even with a `SpeculativeExecutionPolicy` configured on the `Session`,
speculative fibers only start if **the statement is marked idempotent**
(see [Query idempotence](../retry-policy/retry-policy.md#query-idempotence)).
For non-idempotent statements the policy is bypassed and only the original
execution runs - this avoids duplicating side-effecting writes when several
fibers race to the same coordinator. Use `Statement::set_is_idempotent(true)`
or `PreparedStatement::set_is_idempotent(true)` on queries that are safe to
re-run.

### Errors from running fibers

Once speculative fibers are running, the result of each fiber is checked
against an internal classifier. Errors that look transient on this
particular target (for example, a connection-pool error or an attempt that
another node could still satisfy) are ignored so the remaining fibers keep
racing. Errors that mean the *whole request* should fail - not just one
fiber - are returned immediately, and no more speculative attempts are
started. From a user perspective this means some kinds of failure produce a
fast error even when speculative execution is configured: speculative
fibers are an optimization for slow responses, not a substitute for retry
on definitive errors.

```{eval-rst}
.. toctree::
   :hidden:
   :glob:

   simple
   percentile

```