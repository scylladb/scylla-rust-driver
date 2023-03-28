# Making queries

This driver supports all query types available in Scylla:
* [Simple queries](simple.md)
    * Easy to use
    * Poor performance
    * Primitive load balancing
* [Prepared queries](prepared.md)
    * Need to be prepared before use
    * Fast
    * Properly load balanced
* [Batch statements](batch.md)
    * Run multiple queries at once
    * Can be prepared for better performance and load balancing
* [Paged queries](paged.md)
    * Allows to read result in multiple pages when it doesn't fit in a single response
    * Can be prepared for better performance and load balancing

Additionally there is special functionality to enable `USE KEYSPACE` queries:
[USE keyspace](usekeyspace.md)

Queries are fully asynchronous - you can run as many of them in parallel as you wish.

```eval_rst
.. toctree::
   :hidden:
   :glob:

   simple
   values
   result
   prepared
   batch
   paged
   usekeyspace
   schema-agreement
   lwt
   timeouts
```
