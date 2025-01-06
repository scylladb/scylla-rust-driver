# Displaying Query Results

This guide explains how to display query results from the database using the `RowsDisplayer` utility. The `RowsDisplayer` provides a flexible way to format and visualize query results as tables with various customization options, it tries to copy the behavior of the `cqlsh` utility.

## Basic Usage

To display query results, create a `RowsDisplayer` instance and configure its display settings:

```rust
let result: QueryRowsResult = session
    .query_unpaged("SELECT * FROM examples_ks.basic1", &[])
    .await?
    .into_rows_result()?;
let displayer = result.rows_displayer();
println!("{}", displayer);
```

## Display Settings

### Terminal Width

Control the width of the output table:

```rust
displayer.set_terminal_width(80);
```

- Setting width to 0 (default) disables wrapping
- Table will attempt to wrap at specified width while maintaining readability
- Columns are adjusted proportionally when wrapping

### Color Output

Enable or disable colored output:

```rust
displayer.use_color(true);  // Enable colors (default)
displayer.use_color(false); // Disable colors
```

When enabled, different data types are displayed in distinct colors:
- Numbers (integers, decimals, floats): Green
- Text and strings: Yellow
- Collections (lists, sets, maps): Blue
- Errors: Red
- Binary data: Magenta

### Binary Data Display

Configure how BLOB data is displayed using `ByteDisplaying`:

```rust
displayer.set_blob_displaying(ByteDisplaying::Hex);  // Default
displayer.set_blob_displaying(ByteDisplaying::Ascii);
displayer.set_blob_displaying(ByteDisplaying::Dec);
```

Options:
- `Hex`: Display as hexadecimal values (e.g., "0A FF 42")
- `Ascii`: Display as ASCII characters where possible
- `Dec`: Display as decimal values (e.g., "213 7 66")

### Number Formatting

#### Integer Display

Control scientific notation for integers:

```rust
displayer.set_exponent_displaying_integers(true);  // Enable scientific notation
displayer.set_exponent_displaying_integers(false); // Disable (default)
```

#### Floating Point Precision

Set the number of decimal places for floating point numbers:

```rust
displayer.set_floating_point_precision(6);  // Show 6 decimal places (default)
```

## Example Output

Here's an example of how the output might look with default settings:

```
+----------+-------------+----------------+-------------+
| id       | name        | values        | created_at  |
+----------+-------------+----------------+-------------+
| 1        | Example     | [1, 2, 3]     | 2024-01-06 |
| 2        | Test Data   | [4, 5, 6]     | 2024-01-06 |
+----------+-------------+----------------+-------------+
```

## Best Practices

1. **Terminal Width**
   - Set appropriate terminal width for better readability
   - Consider using terminal width detection if available
   - Use 0 width for untruncated output

2. **Color Usage**
   - Enable colors for better type distinction
   - Disable colors when outputting to files or non-terminal destinations
   - Consider user accessibility settings

3. **Binary Data**
   - Choose appropriate blob display format based on data content
   - Use Hex for general binary data
   - Use ASCII when data is known to be text
   - Use Dec for byte-oriented analysis

4. **Number Formatting**
   - Adjust floating point precision based on data requirements
   - Enable scientific notation for very large/small numbers
   - Consider locale-specific formatting needs

## Implementation Details

The displayer uses the following traits internally:
- `Display` for converting values to strings
- Custom formatting traits for specific types

Output is generated using Rust's formatting system (`fmt::Display`), ensuring efficient memory usage and proper error handling.