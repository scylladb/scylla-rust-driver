
# Benchmarks

Each benchmark recreates the table before execution to ensure consistent and isolated results.

- **concurrent insert**

This benchmark spawns concurrent Tokio tasks, each calling `Session::execute_unpaged`
with a prepared statement to insert `n` rows in total containing `uuid` and `int` into the database.
Afterwards, it checks that the number of rows inserted is correct.

- **insert**

This benchmark executes `n` sequential `Session::execute_unpaged` prepared statement calls,
each inserting a single row containing `uuid` and `int`, waiting for the result before executing the next one.

- **concurrent select**

This benchmark first inserts `10` rows containing `uuid` and `int`.
Afterwards it spawns concurrent Tokio tasks, each calling `Session::execute_unpaged`
to select all inserted rows from the database, repeating `n` times in total.

- **select**

This benchmark first inserts 10 rows containing `uuid` and `int`.
Afterwards it executes `n` sequential `Session::execute_unpaged` prepared statement calls,
each selecting all inserted rows, waiting for the result before executing the next one.

- **concurrent deserialization**

This benchmark spawns concurrent Tokio tasks to insert `n` rows containing
`uuid`, `int`, `timeuuid`, `inet`, `date`, `time` into the database using `Session::execute_unpaged`.
Afterwards it spawns concurrent Tokio tasks to select all (`n`) inserted rows from the database `n` times.

- **deserialization**

This benchmark executes `n` sequential `Session::execute_unpaged` calls to insert a single row containing
`uuid`, `int`, `timeuuid`, `inet`, `date`, `time`, waiting for each result before executing the next.
Afterwards it executes `n` sequential `Session::execute_unpaged` calls to select all (`n`) inserted rows.

- **concurrent serialization**

This benchmark spawns concurrent Tokio tasks, each calling `Session::execute_unpaged` with a prepared statement,
to insert `n*n` rows in total containing `uuid`, `int`, `timeuuid`, `inet`, `date`, `time` into the database.

- **serialization**

This benchmark executes `n*n` sequential `Session::execute_unpaged` prepared statement calls,
each inserting a single row containing `uuid`, `int`, `timeuuid`, `inet`, `date`, `time`,
waiting for each result before executing the next.

- **batch**

This benchmark uses `Session::batch` to insert `n` rows containing `uuid` and `int` into the database.
Afterwards, it checks that the number of rows inserted is correct.
