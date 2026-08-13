# Execution profiles

Execution profiles are a way to group various query execution configuration options together. Profiles can be created to represent different workloads, which can be run conveniently on a single session.

The settings that an execution profile encapsulates are [as follows](maximal-example.md):
* consistency
* serial consistency
* request timeout
* load balancing policy
* retry policy
* speculative execution policy

There are two classes of objects related to execution profiles: `ExecutionProfile` and `ExecutionProfileHandle`. The former is simply an immutable set of the settings. The latter is a handle that at particular moment points to some `ExecutionProfile` (but during its lifetime, it can change the profile it points at). Handles are assigned to `Sessions` and `Statements`.\
\
At any moment, handles [can be remapped](remap.md) to point to another `ExecutionProfile`. This allows convenient switching between workloads for all `Sessions` and/or `Statements` that, for instance, share common characteristics.

```{eval-rst}
.. toctree::
   :hidden:
   :glob:

   create-and-use
   maximal-example
   priority
   remap
```