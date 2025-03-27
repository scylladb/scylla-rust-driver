use chrono::{Duration, NaiveDate, NaiveTime, TimeZone, Utc};
use inline_colorization::{color_blue, color_green, color_magenta, color_red, color_yellow};
use num_bigint_04::BigInt;
use scylla_cql::deserialize::result::TypedRowIterator;
use scylla_cql::value::{
    Counter, CqlDate, CqlDecimal, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlValue,
    CqlVarint, Row,
};
use std::fmt;
use tabled::settings::object::Rows;
use tabled::settings::peaker::Priority;
use tabled::settings::{Alignment, Width};
use tabled::{builder::Builder, settings::themes::Colorization, settings::Color, settings::Style};
use uuid::Uuid;

use super::query_result::QueryRowsResult;

/// A utility struct for displaying rows received from the database in a [`QueryRowsResult`].
///
/// This struct provides methods to configure the display settings and format the rows
/// as a table for easier visualization. It supports various display settings such as
/// terminal width, color usage, and formatting of different CQL value types.
///
/// # Example
///
/// ```rust
/// # use scylla::response::query_result::{QueryResult, QueryRowsResult};
/// # use scylla::response::rows_displayer::RowsDisplayer;
/// # fn example(query_result: QueryResult) -> Result<(), Box<dyn std::error::Error>> {
///     let rows_result = query_result.into_rows_result()?;
///     let mut displayer = rows_result.rows_displayer();
///     displayer.set_terminal_width(80);
///     displayer.use_color(true);
///     println!("{}", displayer);
///     # Ok(())
/// # }
/// ```
pub struct RowsDisplayer<'a> {
    query_result: &'a QueryRowsResult,
    display_settings: RowsDisplayerSettings,
}

impl<'a> RowsDisplayer<'a> {
    /// Creates a new `RowsDisplayer` for the given `QueryRowsResult`.
    /// The default settings are:
    /// - terminal width: 0,
    /// - color usage: true,
    /// - byte displaying: `ByteDisplaying::Hex`
    pub fn new(query_result: &'a QueryRowsResult) -> Self {
        Self {
            query_result,
            display_settings: RowsDisplayerSettings::new(),
        }
    }

    /// Sets the terminal width for wrapping the table output.
    /// The table will be wrapped to the given width, if possible.
    /// If the width is set to 0, the table will not be wrapped.
    /// The default value is 0.
    pub fn set_terminal_width(&mut self, terminal_width: usize) {
        self.display_settings.terminal_width = terminal_width;
    }

    /// Enables or disables color in the table output.
    pub fn use_color(&mut self, use_color: bool) {
        self.display_settings.print_in_color = use_color;
    }

    /// Sets the byte display format for blobs.
    /// The default value is `ByteDisplaying::Hex`.
    /// Possible values are:
    /// - `ByteDisplaying::Ascii` - display bytes as ASCII characters,
    /// - `ByteDisplaying::Hex` - display bytes as hexadecimal numbers,
    /// - `ByteDisplaying::Dec` - display bytes as decimal numbers.
    pub fn set_blob_displaying(&mut self, byte_displaying: ByteDisplaying) {
        self.display_settings.byte_displaying = byte_displaying;
    }

    /// Sets the exponent display for integers.
    /// If set to true, integers will be displayed in scientific notation.
    /// The default value is false.
    pub fn set_exponent_displaying_integers(&mut self, exponent_displaying_integers: bool) {
        self.display_settings.exponent_displaying_integers = exponent_displaying_integers;
    }

    /// Sets the exponent display for floats and doubles.
    pub fn set_floating_point_precision(&mut self, precision: usize) {
        self.display_settings.double_precision = precision;
    }
}

// wrappers for scylla datatypes implementing Display
fn get_item_wrapper<'a>(
    item: &'a CqlValue,
    display_settings: &'a RowsDisplayerSettings,
) -> Box<dyn StringConvertible<'a> + 'a> {
    match item {
        CqlValue::Ascii(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::BigInt(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Blob(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Boolean(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Counter(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Decimal(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Double(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Float(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Int(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Text(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Timestamp(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Uuid(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Inet(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::List(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Map(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Set(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::UserDefinedType {
            keyspace: _,
            fields,
            name: _,
        } => Box::new(WrapperDisplay {
            value: fields,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Tuple(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Date(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Duration(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Empty => Box::new(WrapperDisplay {
            value: &None,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::SmallInt(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::TinyInt(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Varint(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Time(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        CqlValue::Timeuuid(value) => Box::new(WrapperDisplay {
            value,
            settings: display_settings,
            cql_value: item,
        }),
        // case not covered by display
        _ => Box::new(WrapperDisplay {
            value: &None,
            settings: display_settings,
            cql_value: item,
        }),
    }
}

impl fmt::Display for RowsDisplayer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let row_iter: TypedRowIterator<'_, '_, Row> = match self.query_result.rows::<Row>() {
            Ok(row_iter) => row_iter,
            Err(_) => return fmt::Result::Err(fmt::Error),
        };

        // put columns names to the table
        let column_names = self
            .query_result
            .column_specs()
            .iter()
            .map(|column_spec| column_spec.name());
        let mut builder: Builder = Builder::new();
        builder.push_record(column_names);

        // put rows to the table
        for row_result in row_iter {
            let row_result: Row = match row_result {
                Ok(row_result) => row_result,
                Err(_) => return fmt::Result::Err(fmt::Error),
            };
            let columns: Vec<Option<CqlValue>> = row_result.columns;
            let mut row_values: Vec<Box<dyn StringConvertible>> = Vec::new();
            let columns = columns.iter().map(|item| {
                let item = match item {
                    Some(item) => item,
                    None => &CqlValue::Empty,
                };
                get_item_wrapper(item, &self.display_settings)
            });
            row_values.extend(columns);

            builder.push_record(row_values);
        }

        // build the table
        let mut binding = builder.build();
        let mut table = binding.with(Style::psql()).with(Alignment::right());
        if self.display_settings.terminal_width != 0 {
            table = table.with(
                Width::wrap(self.display_settings.terminal_width).priority(Priority::max(true)),
            );
        }

        // align and colorize the table
        table.modify(Rows::first(), Alignment::left());

        if self.display_settings.print_in_color {
            table = table.with(Colorization::exact(
                [Color::FG_MAGENTA | Color::BOLD],
                tabled::settings::object::Rows::first(),
            ));
        }

        write!(f, "{}", table)
    }
}

/// Settings for displaying rows in a `RowsDisplayer`.
///
/// This struct holds various configuration options for formatting and displaying
/// rows received from the database. It includes settings for byte display format,
/// exponent display for floats and integers, precision for doubles, color usage,
/// and terminal width for wrapping the table output.
struct RowsDisplayerSettings {
    byte_displaying: ByteDisplaying,    // for blobs
    exponent_displaying_floats: bool,   // for floats
    exponent_displaying_integers: bool, // for integers
    double_precision: usize,            // for doubles
    print_in_color: bool,
    terminal_width: usize,
    colors: ValueColors,
}

impl RowsDisplayerSettings {
    fn new() -> Self {
        Self {
            byte_displaying: ByteDisplaying::Hex,
            exponent_displaying_floats: false,
            exponent_displaying_integers: false,
            double_precision: 5,
            print_in_color: true,
            terminal_width: 0,
            colors: ValueColors::default(),
        }
    }
}

impl Default for RowsDisplayerSettings {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
struct ValueColors {
    text: &'static str,
    error: &'static str,
    blob: &'static str,
    timestamp: &'static str,
    date: &'static str,
    time: &'static str,
    int: &'static str,
    float: &'static str,
    decimal: &'static str,
    inet: &'static str,
    boolean: &'static str,
    uuid: &'static str,
    duration: &'static str,
    collection: &'static str,
}

impl Default for ValueColors {
    fn default() -> Self {
        Self {
            text: color_yellow,     // YELLOW
            error: color_red,       // RED
            blob: color_magenta,    // DARK_MAGENTA (using regular magenta as closest match)
            timestamp: color_green, // GREEN
            date: color_green,      // GREEN
            time: color_green,      // GREEN
            int: color_green,       // GREEN
            float: color_green,     // GREEN
            decimal: color_green,   // GREEN
            inet: color_green,      // GREEN
            boolean: color_green,   // GREEN
            uuid: color_green,      // GREEN
            duration: color_green,  // GREEN
            collection: color_blue, // BLUE
        }
    }
}

// macro for writing colored and bold text
macro_rules! write_colored {
    ($f:expr, $enabled:expr, $color:expr, $($arg:tt)*) => {
        // write bold
        if $enabled {
            write!($f, "{}", inline_colorization::style_bold)?;
            write!($f, "{}", $color)?;
            write!($f, $($arg)*)?;
            write!($f, "{}", inline_colorization::color_reset)?;
            write!($f, "{}", inline_colorization::style_reset)
        } else {
            write!($f, $($arg)*)
        }
    };
}

#[derive(PartialEq)]
pub enum ByteDisplaying {
    Ascii,
    Hex,
    Dec,
}

// wrappers for scylla datatypes implementing Display

/// A wrapper struct for displaying various Scylla CQL value types with custom formatting settings.
///
/// This struct is used to format and display different CQL value types according to the provided
/// display settings. It supports various formatting options such as color usage, byte display format,
/// exponent display for floats and integers, and precision for doubles.
///
/// # Type Parameters
///
/// - `T`: The type of the value to be displayed.
///
/// # Fields
///
/// - `value`: A reference to the value to be displayed.
/// - `settings`: A reference to the display settings to be used for formatting.
///
///
/// This will print the value `42` formatted according to the provided settings.
struct WrapperDisplay<'a, T: 'a> {
    value: &'a T,
    settings: &'a RowsDisplayerSettings,
    cql_value: &'a CqlValue,
}

/// A trait for types that can be converted to a `String`.
///
/// Trait bound to ensure From<WrapperDisplay<T>> for String is implemented
trait StringConvertible<'a>: 'a + fmt::Display {}

// Implement the trait for types that have From<WrapperDisplay<T>> for String
impl<'a, T> StringConvertible<'a> for WrapperDisplay<'a, T> where WrapperDisplay<'a, T>: fmt::Display
{}

// generic impl of From ... for String
impl<'a> From<Box<dyn StringConvertible<'a>>> for String {
    fn from(wrapper: Box<dyn StringConvertible<'a>>) -> Self {
        wrapper.to_string()
    }
}

impl<'a, T> From<WrapperDisplay<'a, T>> for String
where
    T: 'a,
    WrapperDisplay<'a, T>: fmt::Display,
{
    fn from(wrapper: WrapperDisplay<'a, T>) -> Self {
        format!("{}", wrapper)
    }
}

// Actual implementations of Display for scylla datatypes

// none WrapperDisplay
impl fmt::Display for WrapperDisplay<'_, std::option::Option<CqlValue>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.error,
            "null"
        )
    }
}

// tiny int
impl fmt::Display for WrapperDisplay<'_, i8> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.settings.exponent_displaying_floats {
            write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.int,
                "{:e}",
                self.value
            )
        } else {
            write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.int,
                "{}",
                self.value
            )
        }
    }
}

// small int
impl fmt::Display for WrapperDisplay<'_, i16> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.settings.exponent_displaying_floats {
            write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.int,
                "{:e}",
                self.value
            )
        } else {
            write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.int,
                "{}",
                self.value
            )
        }
    }
}

// int
impl fmt::Display for WrapperDisplay<'_, i32> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.settings.exponent_displaying_floats {
            write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.int,
                "{:e}",
                self.value
            )
        } else {
            write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.int,
                "{}",
                self.value
            )
        }
    }
}

// bigint
impl fmt::Display for WrapperDisplay<'_, i64> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.settings.exponent_displaying_floats {
            write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.int,
                "{:e}",
                self.value
            )
        } else {
            write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.int,
                "{}",
                self.value
            )
        }
    }
}

// varint
impl fmt::Display for WrapperDisplay<'_, CqlVarint> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // create a bigint from bytes
        let big_int = BigInt::from_signed_bytes_be(self.value.as_signed_bytes_be_slice());
        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.int,
            "{}",
            big_int
        )
    }
}

// decimal
impl fmt::Display for WrapperDisplay<'_, CqlDecimal> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (big_decimal, exp) = self.value.as_signed_be_bytes_slice_and_exponent();
        let big_int = BigInt::from_signed_bytes_be(big_decimal);
        let big_int = big_int * BigInt::from(10).pow(exp as u32);
        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.decimal,
            "{}",
            big_int
        )
    }
}

// counter
impl fmt::Display for WrapperDisplay<'_, Counter> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.int,
            "{}",
            self.value.0
        )
    }
}

// float
impl fmt::Display for WrapperDisplay<'_, f32> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.settings.exponent_displaying_floats {
            write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.float,
                "{:e}",
                self.value
            )
        } else {
            write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.float,
                "{}",
                self.value
            )
        }
    }
}

// double
impl fmt::Display for WrapperDisplay<'_, f64> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.settings.exponent_displaying_floats {
            write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.float,
                "{:e}",
                self.value
            )
        } else {
            write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.float,
                "{:.digits$}",
                self.value,
                digits = self.settings.double_precision
            )
        }
    }
}

// blob
impl fmt::Display for WrapperDisplay<'_, Vec<u8>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.settings.byte_displaying == ByteDisplaying::Hex {
            write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.blob,
                "0x"
            )?;
        }
        for byte in self.value {
            match self.settings.byte_displaying {
                ByteDisplaying::Ascii => {
                    write_colored!(
                        f,
                        self.settings.print_in_color,
                        self.settings.colors.blob,
                        "{}",
                        *byte as char
                    )?;
                }
                ByteDisplaying::Hex => {
                    write_colored!(
                        f,
                        self.settings.print_in_color,
                        self.settings.colors.blob,
                        "{:02x}",
                        byte
                    )?;
                }
                ByteDisplaying::Dec => {
                    write_colored!(
                        f,
                        self.settings.print_in_color,
                        self.settings.colors.blob,
                        "{}",
                        byte
                    )?;
                }
            }
        }
        Ok(())
    }
}

// string
impl fmt::Display for WrapperDisplay<'_, String> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.text,
            "{}",
            self.value
        )
    }
}

// timestamp
impl fmt::Display for WrapperDisplay<'_, CqlTimestamp> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // example of formating timestamp 14:30:00.000000000
        let seconds_from_epoch = self.value.0;
        let datetime = Utc.timestamp_millis_opt(seconds_from_epoch).single();

        let datetime = match datetime {
            Some(datetime) => datetime,
            None => {
                return write_colored!(
                    f,
                    self.settings.print_in_color,
                    self.settings.colors.error,
                    "Invalid timestamp: {:?} seconds given",
                    seconds_from_epoch
                )
            }
        };

        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.timestamp,
            "{}.{:06}+0000",
            datetime.format("%Y-%m-%d %H:%M:%S"),
            datetime.timestamp_subsec_micros()
        )
    }
}

// time
impl fmt::Display for WrapperDisplay<'_, CqlTime> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // example of formating time 14:30:00.000000000
        let nanoseconds = self.value.0;
        let total_seconds = nanoseconds / 1_000_000_000;
        let hours = total_seconds / 3600;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;
        let nanos = nanoseconds % 1_000_000_000;

        // Create NaiveTime with the calculated components
        let time = NaiveTime::from_hms_nano_opt(
            hours as u32,
            minutes as u32,
            seconds as u32,
            nanos as u32,
        );

        let time = match time {
            Some(time) => time,
            None => {
                return write_colored!(
                    f,
                    self.settings.print_in_color,
                    self.settings.colors.error,
                    "Invalid time: {} hours, {} minutes, {} seconds, {} nanoseconds given",
                    hours,
                    minutes,
                    seconds,
                    nanos
                )
            }
        };

        // Format the time with 9 digits of nanoseconds

        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.time,
            "{}",
            time.format("%H:%M:%S.%9f")
        )
    }
}

// timeuuid
impl fmt::Display for WrapperDisplay<'_, CqlTimeuuid> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let uuid = self.value.as_bytes();

        write_colored!(f, self.settings.print_in_color, self.settings.colors.uuid,
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            uuid[0], uuid[1], uuid[2], uuid[3],
            uuid[4], uuid[5],
            uuid[6], uuid[7],
            uuid[8], uuid[9],
            uuid[10], uuid[11], uuid[12], uuid[13], uuid[14], uuid[15])
    }
}

// duration
impl fmt::Display for WrapperDisplay<'_, CqlDuration> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let if_color = self.settings.print_in_color;
        let color = self.settings.colors.duration;

        let months = self.value.months;
        let days = self.value.days;
        let nanoseconds = self.value.nanoseconds;

        let years = months / 12;
        let months = months % 12;
        let weeks = days / 7;
        let days = days % 7;
        let hours = nanoseconds / 3_600_000_000_000;
        let minutes = (nanoseconds % 3_600_000_000_000) / 60_000_000_000;
        let seconds = (nanoseconds % 60_000_000_000) / 1_000_000_000;
        let nanoseconds = nanoseconds % 1_000_000_000;

        if years != 0 {
            write_colored!(f, if_color, color, "{}y", years)?;
        }

        if months != 0 {
            write_colored!(f, if_color, color, "{}mo", months)?;
        }

        if weeks != 0 {
            write_colored!(f, if_color, color, "{}w", weeks)?;
        }

        if days != 0 {
            write_colored!(f, if_color, color, "{}d", days)?;
        }

        if hours != 0 {
            write_colored!(f, if_color, color, "{}h", hours)?;
        }

        if minutes != 0 {
            write_colored!(f, if_color, color, "{}m", minutes)?;
        }

        if seconds != 0 {
            write_colored!(f, if_color, color, "{}s", seconds)?;
        }

        if nanoseconds != 0 {
            write_colored!(f, if_color, color, "{}ns", nanoseconds)?;
        }
        Ok(())
    }
}

// date
impl fmt::Display for WrapperDisplay<'_, CqlDate> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // example of formating date 2021-12-31
        // CqlDate is Represented as number of days since -5877641-06-23 i.e. 2^31 days before unix epoch.
        let magic_constant = 2055453495; // it is number of days from -5877641-06-23 to -250000-01-01
                                         // around -250000-01-01  is the limit of naive date
                                         // without this constant we can't convert days to date
                                         // because from_ymd_opt will return None

        let days = self.value.0 - magic_constant;
        let base_date = NaiveDate::from_ymd_opt(-250000, 1, 1).unwrap();

        // Add the number of days
        let target_date = base_date
            + match Duration::try_days(days as i64) {
                Some(duration) => duration,
                None => {
                    return write_colored!(
                        f,
                        self.settings.print_in_color,
                        self.settings.colors.error,
                        "Invalid date: {:?} days given",
                        days
                    )
                }
            };

        // Format the date
        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.date,
            "{}",
            target_date.format("%Y-%m-%d")
        )
    }
}

// inet
impl fmt::Display for WrapperDisplay<'_, std::net::IpAddr> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ip = self.value;
        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.inet,
            "{}",
            ip
        )
    }
}

// boolean
impl fmt::Display for WrapperDisplay<'_, bool> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.boolean,
            "{}",
            self.value
        )
    }
}

// Uuid
impl fmt::Display for WrapperDisplay<'_, Uuid> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.uuid,
            "{}",
            self.value
        )
    }
}

fn display_in_quotes_and_colored<'a>(
    f: &mut fmt::Formatter<'_>,
    value: &(dyn StringConvertible<'a> + 'a),
    settings: &RowsDisplayerSettings,
) -> fmt::Result {
    write_colored!(f, settings.print_in_color, settings.colors.text, "'")?;
    write_colored!(
        f,
        settings.print_in_color,
        settings.colors.text,
        "{}",
        value
    )?;
    write_colored!(f, settings.print_in_color, settings.colors.text, "'")
}

// list or set
impl fmt::Display for WrapperDisplay<'_, Vec<CqlValue>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // choose opening bracket
        match self.cql_value {
            CqlValue::List(_) => write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.collection,
                "["
            )?,
            CqlValue::Set(_) => write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.collection,
                "{{"
            )?,
            _ => write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.error,
                "Invalid type of collection. Expected List or Set",
            )?,
        }

        // print elements
        let mut first_ture = true;
        let value: &Vec<CqlValue> = self.value;
        for item in value {
            let item_wrapper = get_item_wrapper(item, self.settings);

            // write comma
            if !first_ture {
                write_colored!(
                    f,
                    self.settings.print_in_color,
                    self.settings.colors.collection,
                    ", "
                )?;
            }
            first_ture = false;

            // if item is text, display it in colored quotes, otherwise display it as is (e.g. number)
            // to not confuse with text
            if matches!(item, CqlValue::Text(_)) {
                display_in_quotes_and_colored(f, &*item_wrapper, self.settings)?;
            } else {
                write!(f, "{}", item_wrapper)?;
            }
        }

        // choose closing bracket
        match self.cql_value {
            CqlValue::List(_) => write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.collection,
                "]"
            )?,
            CqlValue::Set(_) => write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.collection,
                "}}"
            )?,
            _ => write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.error,
                "Invalid type of collection. Expected List or Set",
            )?,
        }
        Ok(())
    }
}

// map
impl fmt::Display for WrapperDisplay<'_, Vec<(CqlValue, CqlValue)>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.collection,
            "{{"
        )?;
        let mut first_item = true; // we want to start printing comma after first item
        let value: &Vec<(CqlValue, CqlValue)> = self.value;
        for (key, value) in value {
            let key_wrapper = get_item_wrapper(key, self.settings);
            let value_wrapper = get_item_wrapper(value, self.settings);
            if !first_item {
                write_colored!(
                    f,
                    self.settings.print_in_color,
                    self.settings.colors.collection,
                    ", "
                )?;
            }

            // if key is text, display it in colored quotes
            if let CqlValue::Text(_key) = key {
                display_in_quotes_and_colored(f, &*key_wrapper, self.settings)?;
            } else {
                write!(f, "{}", key_wrapper)?;
            }

            // display colon
            write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.collection,
                ": "
            )?;

            // if item is text, display it in colored quotes, otherwise display it as is (e.g. number)
            // to not confuse with text
            if let CqlValue::Text(_item) = value {
                display_in_quotes_and_colored(f, &*value_wrapper, self.settings)?;
            } else {
                write!(f, "{}", value_wrapper)?;
            }

            first_item = false;
        }
        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.collection,
            "}}"
        )
    }
}

// udts
impl fmt::Display for WrapperDisplay<'_, Vec<(String, Option<CqlValue>)>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // write opening bracket
        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.collection,
            "{{"
        )?;
        let mut first_item = true; // we want to start printing comma after first item
        let value: &Vec<(String, Option<CqlValue>)> = self.value;
        for (field_name, field_value) in value {
            let field_value_wrapper = match field_value {
                Some(field_value) => get_item_wrapper(field_value, self.settings),
                None => Box::new(WrapperDisplay {
                    value: &None,
                    settings: self.settings,
                    cql_value: &CqlValue::Empty,
                }),
            };

            // write comma
            if !first_item {
                write_colored!(
                    f,
                    self.settings.print_in_color,
                    self.settings.colors.collection,
                    ", "
                )?;
            }
            first_item = false;

            // write field name
            write_colored!(
                f,
                self.settings.print_in_color,
                self.settings.colors.text,
                "{}: ",
                field_name
            )?;

            // if item is text, display it in colored quotes, otherwise display it as is (e.g. number)
            // to not confuse with text
            if let Some(CqlValue::Text(_item)) = field_value {
                display_in_quotes_and_colored(f, &*field_value_wrapper, self.settings)?;
            } else {
                write!(f, "{}", field_value_wrapper)?;
            }
        }

        // write closing bracket
        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.collection,
            "}}"
        )
    }
}

// tuple
impl fmt::Display for WrapperDisplay<'_, Vec<Option<CqlValue>>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.collection,
            "("
        )?;
        let mut first_item = true; // we want to start printing comma after first item
        let value: &Vec<Option<CqlValue>> = self.value;

        // write items
        for item in value {
            let item_wrapper = match item {
                Some(item) => get_item_wrapper(item, self.settings),
                None => Box::new(WrapperDisplay {
                    value: &None,
                    settings: self.settings,
                    cql_value: &CqlValue::Empty,
                }),
            };

            // write comma
            if !first_item {
                write_colored!(
                    f,
                    self.settings.print_in_color,
                    self.settings.colors.collection,
                    ", "
                )?;
            }
            first_item = false;

            // if item is text, display it in colored quotes, otherwise display it as is (e.g. number)
            // to not confuse with text
            if let Some(CqlValue::Text(_item)) = item {
                display_in_quotes_and_colored(f, &*item_wrapper, self.settings)?;
            } else {
                write!(f, "{}", item_wrapper)?;
            }
        }
        // write closing bracket
        write_colored!(
            f,
            self.settings.print_in_color,
            self.settings.colors.collection,
            ")"
        )
    }
}
