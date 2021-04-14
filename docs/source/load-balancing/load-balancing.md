# Load balancing

There are multiple load balancing strategies that the driver can use.  
Load balancing can be configured for the whole `Session` during creation.

Basic load balancing strategies:
* `RoundRobinPolicy` - uses all known nodes one after another
* `DcAwareRoundRobinPolicy` - uses all known nodes from the local datacenter one after another

Each of these basic load balancing strategies can be wrapped in `TokenAwarePolicy` to enable token awareness.

> **Note**  
> Only [prepared queries](../queries/prepared.md) use token aware load balancing

All queries are shard aware, there is no way to turn off shard awareness.  
If a token is available the query is sent to the correct shard, otherwise to a random one.

So, the available load balancing policies are:
* [Round robin](robin.md)
* [DC Aware Round robin](dc-robin.md)
* [Token aware Round robin](token-robin.md)
* [Token aware DC Aware Round robin](token-dc-robin.md)

By default the driver uses `Token aware Round robin`

```eval_rst
.. toctree::
   :hidden:
   :glob:

   robin
   dc-robin
   token-robin
   token-dc-robin

```