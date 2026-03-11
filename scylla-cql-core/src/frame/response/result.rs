//! CQL protocol-level representation of a `RESULT` response.
//!
//! Contains lower-level types that are used to represent the result of a query.

use std::borrow::Cow;
use std::sync::Arc;

/// Specification of a table in a keyspace.
///
/// For a given cluster, [TableSpec] uniquely identifies a table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableSpec<'a> {
    ks_name: Cow<'a, str>,
    table_name: Cow<'a, str>,
}

impl<'a> TableSpec<'a> {
    /// Creates a new borrowed [TableSpec] with the given keyspace and table names.
    pub const fn borrowed(ks: &'a str, table: &'a str) -> Self {
        Self {
            ks_name: Cow::Borrowed(ks),
            table_name: Cow::Borrowed(table),
        }
    }

    /// Retrieves the keyspace name.
    pub fn ks_name(&'a self) -> &'a str {
        self.ks_name.as_ref()
    }

    /// Retrieves the table name.
    pub fn table_name(&'a self) -> &'a str {
        self.table_name.as_ref()
    }

    /// Converts the [TableSpec] to an owned version, where all references are replaced with owned values.
    ///
    /// Does not allocate if the [TableSpec] is already owned.
    pub fn into_owned(self) -> TableSpec<'static> {
        TableSpec::owned(self.ks_name.into_owned(), self.table_name.into_owned())
    }

    /// Converts the [TableSpec] to an owned version, where all references are replaced with owned values.
    ///
    /// Allocates a new [TableSpec] even if the original is already owned.
    pub fn to_owned(&self) -> TableSpec<'static> {
        TableSpec::owned(self.ks_name().to_owned(), self.table_name().to_owned())
    }
}

impl TableSpec<'static> {
    /// Creates a new owned [TableSpec] with the given keyspace and table names.
    pub fn owned(ks_name: String, table_name: String) -> Self {
        Self {
            ks_name: Cow::Owned(ks_name),
            table_name: Cow::Owned(table_name),
        }
    }
}

/// A type of:
/// - a column in schema metadata
/// - a bind marker in a prepared statement
/// - a column a in query result set
///
/// Some of the variants contain a `frozen` flag. This flag is only used
/// in schema metadata. For prepared statement bind markers and query result
/// types those fields will always be set to `false` (even if the DB column
/// corresponding to given marker / result type is frozen).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ColumnType<'frame> {
    /// Types that are "simple" (non-recursive).
    Native(NativeType),

    /// Collection types: Map, Set, and List. Those are composite types with
    /// dynamic size but constant predefined element types.
    Collection {
        /// If a collection is not frozen, elements in the collection can be updated individually.
        /// If it is frozen, the entire collection will be overwritten when it is updated.
        /// A collection cannot contain another collection unless the inner collection is frozen.
        /// The frozen type will be treated as a blob.
        frozen: bool,
        /// Type of the collection's elements.
        typ: CollectionType<'frame>,
    },

    /// A composite list-like type that has a defined size and all its elements
    /// are of the same type. Intuitively, it can be viewed as a list with constant
    /// predefined size, or as a tuple which has all elements of the same type.
    Vector {
        /// Type of the vector's elements.
        typ: Box<ColumnType<'frame>>,
        /// Length of the vector.
        dimensions: u16,
    },

    /// A C-struct-like type defined by the user.
    UserDefinedType {
        /// Analogous to [ColumnType::Collection::frozen].
        /// If a UDT is not frozen, elements in the UDT can be updated individually.
        /// If it is frozen, the entire UDT will be overwritten when it is updated.
        /// A UDT cannot contain another UDT unless the inner UDT is frozen.
        /// The frozen type will be treated as a blob.
        frozen: bool,
        /// Definition of the user-defined type.
        definition: Arc<UserDefinedType<'frame>>,
    },

    /// A composite type with a defined size and elements of possibly different,
    /// but predefined, types.
    Tuple(Vec<ColumnType<'frame>>),
}

/// A [ColumnType] variants that are "simple" (non-recursive).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum NativeType {
    /// ASCII-only string.
    Ascii,
    /// Boolean value.
    Boolean,
    /// Binary data of any length.
    Blob,
    /// Counter value, represented as a 64-bit integer.
    Counter,
    /// Days since -5877641-06-23 i.e. 2^31 days before unix epoch
    Date,
    /// Variable-precision decimal.
    Decimal,
    /// 64-bit IEEE-754 floating point number.
    Double,
    /// A duration with nanosecond precision.
    Duration,
    /// 32-bit IEEE-754 floating point number.
    Float,
    /// 32-bit signed integer.
    Int,
    /// 64-bit signed integer.
    BigInt,
    /// UTF-8 encoded string.
    Text,
    /// Milliseconds since unix epoch.
    Timestamp,
    /// IPv4 or IPv6 address.
    Inet,
    /// 16-bit signed integer.
    SmallInt,
    /// 8-bit signed integer.
    TinyInt,
    /// Nanoseconds since midnight.
    Time,
    /// Version 1 UUID, generally used as a "conflict-free" timestamp.
    Timeuuid,
    /// Universally unique identifier (UUID) of any version.
    Uuid,
    /// Arbitrary-precision integer.
    Varint,
}

impl NativeType {
    /// This function returns the size of the type as it is used by Cassandra
    /// for the purposes of serialization and deserialization of vectors,
    /// it is needed as the variable size types (`None`) are de/serialized
    /// differently than the fixed size types (`Some(size)`). Note that
    /// many fixed size types are treated as variable size by Cassandra.
    pub fn type_size_for_vector(&self) -> Option<usize> {
        match self {
            NativeType::Ascii => None,
            NativeType::Boolean => Some(1),
            NativeType::Blob => None,
            NativeType::Counter => None,
            NativeType::Date => None,
            NativeType::Decimal => None,
            NativeType::Double => Some(8),
            NativeType::Duration => None,
            NativeType::Float => Some(4),
            NativeType::Int => Some(4),
            NativeType::BigInt => Some(8),
            NativeType::Text => None,
            NativeType::Timestamp => Some(8),
            NativeType::Inet => None,
            NativeType::SmallInt => None,
            NativeType::TinyInt => None,
            NativeType::Time => None,
            NativeType::Timeuuid => Some(16),
            NativeType::Uuid => Some(16),
            NativeType::Varint => None,
        }
    }
}

/// Collection variants of [ColumnType]. A collection is a composite type that
/// has dynamic size, so it is possible to add and remove values to/from it.
///
/// Tuple and vector are not collections because they have predefined size.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CollectionType<'frame> {
    /// A list of CQL values of the same types.
    List(Box<ColumnType<'frame>>),
    /// A map of CQL values, whose all keys have the same type
    /// and all values have the same type.
    Map(Box<ColumnType<'frame>>, Box<ColumnType<'frame>>),
    /// A set of CQL values of the same types.
    Set(Box<ColumnType<'frame>>),
}

/// Definition of a user-defined type (UDT).
/// UDT is composed of fields, each with a name and an optional value of its own type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserDefinedType<'frame> {
    /// Name of the user-defined type.
    pub name: Cow<'frame, str>,
    /// Keyspace the type belongs to.
    pub keyspace: Cow<'frame, str>,
    /// Fields of the user-defined type - (name, type) pairs.
    pub field_types: Vec<(Cow<'frame, str>, ColumnType<'frame>)>,
}

impl ColumnType<'_> {
    /// Converts a [ColumnType] to an owned version, where all
    /// references are replaced with owned values.
    ///
    /// Unfortunately, this allocates even if the type is already owned.
    pub fn into_owned(self) -> ColumnType<'static> {
        match self {
            ColumnType::Native(b) => ColumnType::Native(b),
            ColumnType::Collection { frozen, typ: t } => ColumnType::Collection {
                frozen,
                typ: t.into_owned(),
            },
            ColumnType::Vector {
                typ: type_,
                dimensions,
            } => ColumnType::Vector {
                typ: Box::new(type_.into_owned()),
                dimensions,
            },
            ColumnType::UserDefinedType {
                frozen,
                definition: udt,
            } => {
                let udt = Arc::try_unwrap(udt).unwrap_or_else(|e| e.as_ref().clone());
                ColumnType::UserDefinedType {
                    frozen,
                    definition: Arc::new(UserDefinedType {
                        name: udt.name.into_owned().into(),
                        keyspace: udt.keyspace.into_owned().into(),
                        field_types: udt
                            .field_types
                            .into_iter()
                            .map(|(cow, column_type)| {
                                (cow.into_owned().into(), column_type.into_owned())
                            })
                            .collect(),
                    }),
                }
            }
            ColumnType::Tuple(vec) => {
                ColumnType::Tuple(vec.into_iter().map(ColumnType::into_owned).collect())
            }
        }
    }

    /// Returns the size of the type in bytes, as it is seen by the vector type if it is treated as fixed size.
    pub fn type_size_for_vector(&self) -> Option<usize> {
        match self {
            ColumnType::Native(n) => n.type_size_for_vector(),
            ColumnType::Tuple(_) => None,
            ColumnType::Collection { .. } => None,
            ColumnType::Vector { typ, dimensions } => typ
                .type_size_for_vector()
                .map(|size| size * usize::from(*dimensions)),
            ColumnType::UserDefinedType { .. } => None,
        }
    }

    /// Returns true if the type allows a special, empty value in addition to its
    /// natural representation. For example, bigint represents a 64-bit integer,
    /// but it can also hold a 0-bit empty value.
    ///
    /// It looks like Cassandra 4.1.3 rejects empty values for some more types than
    /// Scylla: date, time, smallint and tinyint. We will only check against
    /// Scylla's set of types supported for empty values as it's smaller;
    /// with Cassandra, some rejects will just have to be rejected on the db side.
    pub fn supports_special_empty_value(&self) -> bool {
        #[expect(clippy::match_like_matches_macro)]
        match self {
            ColumnType::Native(NativeType::Counter)
            | ColumnType::Native(NativeType::Duration)
            | ColumnType::Collection { .. }
            | ColumnType::UserDefinedType { .. } => false,

            _ => true,
        }
    }
}

impl CollectionType<'_> {
    /// Converts a [CollectionType] to an owned version, where all
    /// references are replaced with owned values.
    ///
    /// Unfortunately, this allocates even if the type is already owned.
    fn into_owned(self) -> CollectionType<'static> {
        match self {
            CollectionType::List(elem_type) => {
                CollectionType::List(Box::new(elem_type.into_owned()))
            }
            CollectionType::Map(key, value) => {
                CollectionType::Map(Box::new(key.into_owned()), Box::new(value.into_owned()))
            }
            CollectionType::Set(elem_type) => CollectionType::Set(Box::new(elem_type.into_owned())),
        }
    }
}

/// Specification of a column of a table.
///
/// For a given cluster, [ColumnSpec] uniquely identifies a column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec<'frame> {
    table_spec: TableSpec<'frame>,
    name: Cow<'frame, str>,
    typ: ColumnType<'frame>,
}

impl ColumnSpec<'static> {
    /// Creates a new owned [ColumnSpec] with the given name, type, and table specification.
    #[inline]
    pub fn owned(name: String, typ: ColumnType<'static>, table_spec: TableSpec<'static>) -> Self {
        Self {
            table_spec,
            name: Cow::Owned(name),
            typ,
        }
    }
}

impl<'frame> ColumnSpec<'frame> {
    /// Creates a new borrowed [ColumnSpec] with the given name, type, and table specification.
    #[inline]
    pub const fn borrowed(
        name: &'frame str,
        typ: ColumnType<'frame>,
        table_spec: TableSpec<'frame>,
    ) -> Self {
        Self {
            table_spec,
            name: Cow::Borrowed(name),
            typ,
        }
    }

    /// Retrieves the table specification associated with this column.
    #[inline]
    pub fn table_spec(&self) -> &TableSpec<'frame> {
        &self.table_spec
    }

    /// Retrieves the name of the column.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Retrieves the type of the column.
    #[inline]
    pub fn typ(&self) -> &ColumnType<'frame> {
        &self.typ
    }

    /// Converts the [ColumnSpec] to an owned version, where all references are replaced with owned values.
    pub fn into_owned(self) -> ColumnSpec<'static> {
        ColumnSpec::owned(
            self.name.into_owned(),
            self.typ.into_owned(),
            self.table_spec.into_owned(),
        )
    }
}

/// Represents the relationship between partition key columns and bind markers.
/// This allows implementations with token-aware routing to correctly
/// construct the partition key without needing to inspect table
/// metadata.
///
/// For example, `PartitionKeyIndex { index: 2, sequence: 1 }` means
/// that the third bind marker is the second column of the partition key.
#[derive(Debug, Copy, Clone)]
pub struct PartitionKeyIndex {
    /// Index of the bind marker.
    pub index: u16,
    /// Sequence number in partition key.
    pub sequence: u16,
}
