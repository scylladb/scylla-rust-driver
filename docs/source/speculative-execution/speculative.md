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

```eval_rst
.. toctree::
   :hidden:
   :glob:

   simple
   percentile

```