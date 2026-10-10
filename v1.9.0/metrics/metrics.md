# Driver metrics

This feature is available only under the crate feature `metrics`.

During operation the driver collects various metrics.

They can be accessed at any moment using `Session::get_metrics()`.

Please note that those are driver-side metrics, not server-side.
This means that latencies reported here will be different, because they also include
the round trip time of a request and driver-side processing.

If you need server-side metrics, please look into Scylla Monitoring Stack.

## Collected metrics:

* Request latencies
* Total number of unpaged requests, and number of errors among them
* Total number of manually paged requests (single page fetched per request),
  and number of errors among them
* Total number of automatically paged requests (`QueryPager` / `*_iter()` APIs),
  counted once per page fetched, and number of errors among them
* Number of retries
* Latency histogram statistics (min, max, mean, standard deviation, percentiles)
* Rates of requests per second in various time frames
* Number of active connections, and connection and request timeouts

## Example

```rust
let metrics = session.get_metrics();

println!(
    "Unpaged requests: {}",
    metrics.get_requests_unpaged_num()
);
println!(
    "Manually paged requests: {}",
    metrics.get_requests_manually_paged_num()
);
println!(
    "Automatically paged requests: {}",
    metrics.get_requests_automatically_paged_num()
);
println!(
    "Errors occurred in unpaged requests: {}",
    metrics.get_errors_unpaged_num()
);
println!(
    "Errors occurred in manually paged requests: {}",
    metrics.get_errors_manually_paged_num()
);
println!(
    "Errors occurred in automatically paged requests: {}",
    metrics.get_errors_automatically_paged_num()
);
println!("Average latency: {}", metrics.get_latency_avg_ms()?);
println!(
    "99 latency percentile: {}",
    metrics.get_latency_percentile_ms(99.0)?
);

let snapshot = metrics.get_snapshot()?;
println!("Min: {}", snapshot.min);
println!("Max: {}", snapshot.max);
println!("Mean: {}", snapshot.mean);
println!("Standard deviation: {}", snapshot.stddev);
println!("Median: {}", snapshot.median);
println!("75th percentile: {}", snapshot.percentile_75);
println!("95th percentile: {}", snapshot.percentile_95);
println!("98th percentile: {}", snapshot.percentile_98);
println!("99th percentile: {}", snapshot.percentile_99);
println!("99.9th percentile: {}", snapshot.percentile_99_9);

println!("Mean rate: {}", metrics.get_mean_rate());
println!("One minute rate: {}", metrics.get_one_minute_rate());
println!("Five minute rate: {}", metrics.get_five_minute_rate());
println!("Fifteen minute rate: {}", metrics.get_fifteen_minute_rate());

println!("Total connections: {}", metrics.get_total_connections());
println!("Connection timeouts: {}", metrics.get_connection_timeouts());
println!("Requests timeouts: {}", metrics.get_request_timeouts());
```

The full [example](https://github.com/scylladb/scylla-rust-driver/tree/main/examples/metrics.rs) is available in the `examples` folder.
You can run it from main folder of driver repository using `SCYLLA_URI=<scylla_ip>:9042 cargo run --example metrics`.
