use crate::deserialize::result::{RawRowIterator, TypedRowIterator};
use crate::deserialize::row::DeserializeRow;
use crate::deserialize::{FrameSlice, TypeCheckError};
use crate::frame::frame_errors::{
    ColumnSpecParseError, ColumnSpecParseErrorKind, CqlResultParseError, CqlTypeParseError,
    LowLevelDeserializationError, PreparedMetadataParseError, PreparedParseError,
    RawRowsAndPagingStateResponseParseError, ResultMetadataAndRowsCountParseError,
    ResultMetadataParseError, SchemaChangeEventParseError, SetKeyspaceParseError,
    TableSpecParseError,
};
use crate::frame::request::query::PagingStateResponse;
use crate::frame::response::custom_type_parser;
use crate::frame::response::event::SchemaChangeEvent;
use crate::frame::types;
use bytes::{Buf, Bytes};
use std::borrow::Cow;
use std::fmt::Debug;
use std::sync::Arc;
use std::{result::Result as StdResult, str};

#[derive(Debug)]
pub struct SetKeyspace {
    pub keyspace_name: String,
}

#[derive(Debug)]
pub struct Prepared {
    pub id: Bytes,
    pub prepared_metadata: PreparedMetadata,
    pub result_metadata: ResultMetadata<'static>,
}

#[derive(Debug)]
pub struct SchemaChange {
    pub event: SchemaChangeEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableSpec<'a> {
    ks_name: Cow<'a, str>,
    table_name: Cow<'a, str>,
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
        frozen: bool,
        typ: CollectionType<'frame>,
    },

    /// A composite list-like type that has a defined size and all its elements
    /// are of the same type. Intuitively, it can be viewed as a list with constant
    /// predefined size, or as a tuple which has all elements of the same type.
    Vector {
        typ: Box<ColumnType<'frame>>,
        dimensions: u16,
    },

    /// A C-struct-like type defined by the user.
    UserDefinedType {
        frozen: bool,
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
    Ascii,
    Boolean,
    Blob,
    Counter,
    Date,
    Decimal,
    Double,
    Duration,
    Float,
    Int,
    BigInt,
    Text,
    Timestamp,
    Inet,
    SmallInt,
    TinyInt,
    Time,
    Timeuuid,
    Uuid,
    Varint,
}

/// Collection variants of [ColumnType]. A collection is a composite type that
/// has dynamic size, so it is possible to add and remove values to/from it.
///
/// Tuple and vector are not collections because they have predefined size.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CollectionType<'frame> {
    List(Box<ColumnType<'frame>>),
    Map(Box<ColumnType<'frame>>, Box<ColumnType<'frame>>),
    Set(Box<ColumnType<'frame>>),
}

/// Definition of a user-defined type
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserDefinedType<'frame> {
    pub name: Cow<'frame, str>,
    pub keyspace: Cow<'frame, str>,
    pub field_types: Vec<(Cow<'frame, str>, ColumnType<'frame>)>,
}

impl ColumnType<'_> {
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
}

impl CollectionType<'_> {
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

impl<'a> TableSpec<'a> {
    pub const fn borrowed(ks: &'a str, table: &'a str) -> Self {
        Self {
            ks_name: Cow::Borrowed(ks),
            table_name: Cow::Borrowed(table),
        }
    }

    pub fn ks_name(&'a self) -> &'a str {
        self.ks_name.as_ref()
    }

    pub fn table_name(&'a self) -> &'a str {
        self.table_name.as_ref()
    }

    pub fn into_owned(self) -> TableSpec<'static> {
        TableSpec::owned(self.ks_name.into_owned(), self.table_name.into_owned())
    }

    pub fn to_owned(&self) -> TableSpec<'static> {
        TableSpec::owned(self.ks_name().to_owned(), self.table_name().to_owned())
    }
}

impl TableSpec<'static> {
    pub fn owned(ks_name: String, table_name: String) -> Self {
        Self {
            ks_name: Cow::Owned(ks_name),
            table_name: Cow::Owned(table_name),
        }
    }
}

impl ColumnType<'_> {
    // Returns true if the type allows a special, empty value in addition to its
    // natural representation. For example, bigint represents a 32-bit integer,
    // but it can also hold a 0-bit empty value.
    //
    // It looks like Cassandra 4.1.3 rejects empty values for some more types than
    // Scylla: date, time, smallint and tinyint. We will only check against
    // Scylla's set of types supported for empty values as it's smaller;
    // with Cassandra, some rejects will just have to be rejected on the db side.
    pub(crate) fn supports_special_empty_value(&self) -> bool {
        #[allow(clippy::match_like_matches_macro)]
        match self {
            ColumnType::Native(NativeType::Counter)
            | ColumnType::Native(NativeType::Duration)
            | ColumnType::Collection { .. }
            | ColumnType::UserDefinedType { .. } => false,

            _ => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec<'frame> {
    pub(crate) table_spec: TableSpec<'frame>,
    pub(crate) name: Cow<'frame, str>,
    pub(crate) typ: ColumnType<'frame>,
}

impl ColumnSpec<'static> {
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

    #[inline]
    pub fn table_spec(&self) -> &TableSpec<'frame> {
        &self.table_spec
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn typ(&self) -> &ColumnType<'frame> {
        &self.typ
    }
}

#[derive(Debug, Clone)]
pub struct ResultMetadata<'a> {
    col_count: usize,
    col_specs: Vec<ColumnSpec<'a>>,
}

impl<'a> ResultMetadata<'a> {
    #[inline]
    pub fn col_count(&self) -> usize {
        self.col_count
    }

    #[inline]
    pub fn col_specs(&self) -> &[ColumnSpec<'a>] {
        &self.col_specs
    }

    // Preferred to implementing Default, because users shouldn't be encouraged to create
    // empty ResultMetadata.
    #[inline]
    pub fn mock_empty() -> Self {
        Self {
            col_count: 0,
            col_specs: Vec::new(),
        }
    }
}

/// Versatile container for [ResultMetadata]. Allows 2 types of ownership
/// of `ResultMetadata`:
/// 1. owning it in a borrowed form, self-borrowed from the RESULT:Rows frame;
/// 2. sharing ownership of metadata cached in PreparedStatement.
#[derive(Debug)]
pub enum ResultMetadataHolder {
    SelfBorrowed(SelfBorrowedMetadataContainer),
    SharedCached(Arc<ResultMetadata<'static>>),
}

impl ResultMetadataHolder {
    /// Returns reference to the stored [ResultMetadata].
    ///
    /// Note that [ResultMetadataHolder] cannot implement [Deref](std::ops::Deref),
    /// because `Deref` does not permit that `Deref::Target`'s lifetime depend on
    /// lifetime of `&self`.
    #[inline]
    pub fn inner(&self) -> &ResultMetadata<'_> {
        match self {
            ResultMetadataHolder::SelfBorrowed(c) => c.metadata(),
            ResultMetadataHolder::SharedCached(s) => s,
        }
    }

    /// Creates an empty [ResultMetadataHolder].
    #[inline]
    pub fn mock_empty() -> Self {
        Self::SelfBorrowed(SelfBorrowedMetadataContainer::mock_empty())
    }
}

#[derive(Debug, Copy, Clone)]
pub struct PartitionKeyIndex {
    /// index in the serialized values
    pub index: u16,
    /// sequence number in partition key
    pub sequence: u16,
}

#[derive(Debug, Clone)]
pub struct PreparedMetadata {
    pub flags: i32,
    pub col_count: usize,
    /// pk_indexes are sorted by `index` and can be reordered in partition key order
    /// using `sequence` field
    pub pk_indexes: Vec<PartitionKeyIndex>,
    pub col_specs: Vec<ColumnSpec<'static>>,
}

/// RESULT:Rows response, in partially serialized form.
///
/// Flags and paging state are deserialized, remaining part of metadata
/// as well as rows remain serialized.
#[derive(Debug, Clone)]
pub struct RawMetadataAndRawRows {
    // Already deserialized part of metadata:
    col_count: usize,
    global_tables_spec: bool,
    no_metadata: bool,

    /// The remaining part of the RESULT frame.
    raw_metadata_and_rows: Bytes,

    /// Metadata cached in PreparedStatement, if present.
    cached_metadata: Option<Arc<ResultMetadata<'static>>>,
}

impl RawMetadataAndRawRows {
    /// Creates an empty [RawMetadataAndRawRows].
    // Preferred to implementing Default, because users shouldn't be encouraged to create
    // empty RawMetadataAndRawRows.
    #[inline]
    pub fn mock_empty() -> Self {
        // Minimal correct `raw_metadata_and_rows` looks like this:
        // Empty metadata (0 bytes), rows_count=0 (i32 big endian), empty rows (0 bytes).
        static EMPTY_METADATA_ZERO_ROWS: &[u8] = &0_i32.to_be_bytes();
        let raw_metadata_and_rows = Bytes::from_static(EMPTY_METADATA_ZERO_ROWS);

        Self {
            col_count: 0,
            global_tables_spec: false,
            no_metadata: false,
            raw_metadata_and_rows,
            cached_metadata: None,
        }
    }

    /// Returns the serialized size of the raw metadata + raw rows.
    #[inline]
    pub fn metadata_and_rows_bytes_size(&self) -> usize {
        self.raw_metadata_and_rows.len()
    }
}

mod self_borrowed_metadata {
    use std::ops::Deref;

    use bytes::Bytes;
    use yoke::{Yoke, Yokeable};

    use super::ResultMetadata;

    // A trivial wrapper over Bytes, introduced to circumvent the orphan rule.
    // (neither `bytes` nor `stable_deref_trait` crate wants to implement
    //  `StableDeref` for `Bytes`, so we need a wrapper for that)
    #[derive(Debug, Clone)]
    struct BytesWrapper {
        inner: Bytes,
    }

    impl Deref for BytesWrapper {
        type Target = [u8];

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    // SAFETY:
    // StableDeref requires that a type dereferences to a stable address, even when moved.
    // `Bytes` satisfy this requirement, because they dereference to their heap allocation.
    unsafe impl stable_deref_trait::StableDeref for BytesWrapper {}

    // SAFETY:
    // Citing `CloneableCart`'s docstring:
    // > Safety
    // > This trait is safe to implement on StableDeref types which, once Cloned, point to the same underlying data and retain ownership.
    //
    // `Bytes` satisfy this requirement.
    unsafe impl yoke::CloneableCart for BytesWrapper {}

    // A trivial wrapper over [ResultMetadata], introduced to keep ResultMetadata free of Yoke.
    // This way Yoke does not appear in any public types/APIs.
    #[derive(Debug, Clone, Yokeable)]
    struct ResultMetadataWrapper<'frame>(ResultMetadata<'frame>);

    /// A container that can be considered an `Arc<ResultMetadata>` with an additional capability
    /// of containing metadata in a borrowed form.
    ///
    /// The borrow comes from the `Bytes` that this container holds internally. Therefore,
    /// the held `ResultMetadata`'s lifetime is covariant with the lifetime of this container
    /// itself.
    #[derive(Debug, Clone)]
    pub struct SelfBorrowedMetadataContainer {
        metadata_and_raw_rows: Yoke<ResultMetadataWrapper<'static>, BytesWrapper>,
    }

    impl SelfBorrowedMetadataContainer {
        /// Creates an empty [SelfBorrowedMetadataContainer].
        pub fn mock_empty() -> Self {
            Self {
                metadata_and_raw_rows: Yoke::attach_to_cart(
                    BytesWrapper {
                        inner: Bytes::new(),
                    },
                    |_| ResultMetadataWrapper(ResultMetadata::mock_empty()),
                ),
            }
        }

        /// Returns a reference to the contained [ResultMetadata].
        pub fn metadata(&self) -> &ResultMetadata<'_> {
            &self.metadata_and_raw_rows.get().0
        }

        // Returns Self (deserialized metadata) and the rest of the bytes,
        // which contain rows count and then rows themselves.
        pub(super) fn make_deserialized_metadata<F, ErrorT>(
            frame: Bytes,
            deserializer: F,
        ) -> Result<(Self, Bytes), ErrorT>
        where
            // This constraint is modelled after `Yoke::try_attach_to_cart`.
            F: for<'frame> FnOnce(&mut &'frame [u8]) -> Result<ResultMetadata<'frame>, ErrorT>,
        {
            let deserialized_metadata_and_raw_rows: Yoke<
                (ResultMetadataWrapper<'static>, &'static [u8]),
                BytesWrapper,
            > = Yoke::try_attach_to_cart(BytesWrapper { inner: frame }, |mut slice| {
                let metadata = deserializer(&mut slice)?;
                let row_count_and_raw_rows = slice;
                Ok((ResultMetadataWrapper(metadata), row_count_and_raw_rows))
            })?;

            let (_metadata, raw_rows) = deserialized_metadata_and_raw_rows.get();
            let raw_rows_with_count = deserialized_metadata_and_raw_rows
                .backing_cart()
                .inner
                .slice_ref(raw_rows);

            Ok((
                Self {
                    metadata_and_raw_rows: deserialized_metadata_and_raw_rows
                        .map_project(|(metadata, _), _| metadata),
                },
                raw_rows_with_count,
            ))
        }
    }
}
pub use self_borrowed_metadata::SelfBorrowedMetadataContainer;

/// RESULT:Rows response, in partially serialized form.
///
/// Paging state and metadata are deserialized, rows remain serialized.
#[derive(Debug)]
pub struct DeserializedMetadataAndRawRows {
    metadata: ResultMetadataHolder,
    rows_count: usize,
    raw_rows: Bytes,
}

impl DeserializedMetadataAndRawRows {
    /// Returns the metadata associated with this response
    /// (table and column specifications).
    #[inline]
    pub fn metadata(&self) -> &ResultMetadata<'_> {
        self.metadata.inner()
    }

    /// Consumes the `DeserializedMetadataAndRawRows` and returns metadata
    /// associated with the response (or cached metadata, if used in its stead).
    #[inline]
    pub fn into_metadata(self) -> ResultMetadataHolder {
        self.metadata
    }

    /// Returns the number of rows that the RESULT:Rows contain.
    #[inline]
    pub fn rows_count(&self) -> usize {
        self.rows_count
    }

    /// Returns the serialized size of the raw rows.
    #[inline]
    pub fn rows_bytes_size(&self) -> usize {
        self.raw_rows.len()
    }

    // Preferred to implementing Default, because users shouldn't be encouraged to create
    // empty DeserializedMetadataAndRawRows.
    #[inline]
    pub fn mock_empty() -> Self {
        Self {
            metadata: ResultMetadataHolder::SelfBorrowed(
                SelfBorrowedMetadataContainer::mock_empty(),
            ),
            rows_count: 0,
            raw_rows: Bytes::new(),
        }
    }

    pub(crate) fn into_inner(self) -> (ResultMetadataHolder, usize, Bytes) {
        (self.metadata, self.rows_count, self.raw_rows)
    }

    /// Creates a typed iterator over the rows that lazily deserializes
    /// rows in the result.
    ///
    /// Returns Err if the schema of returned result doesn't match R.
    #[inline]
    pub fn rows_iter<'frame, 'metadata, R: DeserializeRow<'frame, 'metadata>>(
        &'frame self,
    ) -> StdResult<TypedRowIterator<'frame, 'metadata, R>, TypeCheckError>
    where
        'frame: 'metadata,
    {
        let frame_slice = FrameSlice::new(&self.raw_rows);
        let raw = RawRowIterator::new(
            self.rows_count,
            self.metadata.inner().col_specs(),
            frame_slice,
        );
        TypedRowIterator::new(raw)
    }
}

#[derive(Debug)]
pub enum Result {
    Void,
    Rows((RawMetadataAndRawRows, PagingStateResponse)),
    SetKeyspace(SetKeyspace),
    Prepared(Prepared),
    SchemaChange(SchemaChange),
}

fn deser_type_generic<'frame, 'result, StrT: Into<Cow<'result, str>>>(
    buf: &mut &'frame [u8],
    read_string: fn(&mut &'frame [u8]) -> StdResult<StrT, LowLevelDeserializationError>,
) -> StdResult<ColumnType<'result>, CqlTypeParseError> {
    use ColumnType::*;
    use NativeType::*;
    let id =
        types::read_short(buf).map_err(|err| CqlTypeParseError::TypeIdParseError(err.into()))?;
    Ok(match id {
        0x0000 => {
            let type_str = read_string(buf).map_err(CqlTypeParseError::CustomTypeNameParseError)?;
            let type_cow: Cow<'result, str> = type_str.into();
            let result = match type_cow {
                Cow::Borrowed(type_name) => custom_type_parser::CustomTypeParser::parse(type_name),
                Cow::Owned(type_name) => Ok(custom_type_parser::CustomTypeParser::parse(
                    type_name.as_str(),
                )?
                .into_owned()),
            };
            if let Ok(typ) = result {
                typ
            } else {
                return Err(CqlTypeParseError::TypeNotImplemented(id));
            }
        }
        0x0001 => Native(Ascii),
        0x0002 => Native(BigInt),
        0x0003 => Native(Blob),
        0x0004 => Native(Boolean),
        0x0005 => Native(Counter),
        0x0006 => Native(Decimal),
        0x0007 => Native(Double),
        0x0008 => Native(Float),
        0x0009 => Native(Int),
        0x000B => Native(Timestamp),
        0x000C => Native(Uuid),
        0x000D => Native(Text),
        0x000E => Native(Varint),
        0x000F => Native(Timeuuid),
        0x0010 => Native(Inet),
        0x0011 => Native(Date),
        0x0012 => Native(Time),
        0x0013 => Native(SmallInt),
        0x0014 => Native(TinyInt),
        0x0015 => Native(Duration),
        0x0020 => Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(deser_type_generic(buf, read_string)?)),
        },
        0x0021 => Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(deser_type_generic(buf, read_string)?),
                Box::new(deser_type_generic(buf, read_string)?),
            ),
        },
        0x0022 => Collection {
            frozen: false,
            typ: CollectionType::Set(Box::new(deser_type_generic(buf, read_string)?)),
        },
        0x0030 => {
            let keyspace_name =
                read_string(buf).map_err(CqlTypeParseError::UdtKeyspaceNameParseError)?;
            let type_name = read_string(buf).map_err(CqlTypeParseError::UdtNameParseError)?;
            let fields_size: usize = types::read_short(buf)
                .map_err(|err| CqlTypeParseError::UdtFieldsCountParseError(err.into()))?
                .into();

            let mut field_types: Vec<(Cow<'result, str>, ColumnType)> =
                Vec::with_capacity(fields_size);

            for _ in 0..fields_size {
                let field_name =
                    read_string(buf).map_err(CqlTypeParseError::UdtFieldNameParseError)?;
                let field_type = deser_type_generic(buf, read_string)?;

                field_types.push((field_name.into(), field_type));
            }

            UserDefinedType {
                frozen: false,
                definition: Arc::new(self::UserDefinedType {
                    name: type_name.into(),
                    keyspace: keyspace_name.into(),
                    field_types,
                }),
            }
        }
        0x0031 => {
            let len: usize = types::read_short(buf)
                .map_err(|err| CqlTypeParseError::TupleLengthParseError(err.into()))?
                .into();
            let mut types = Vec::with_capacity(len);
            for _ in 0..len {
                types.push(deser_type_generic(buf, read_string)?);
            }
            Tuple(types)
        }
        id => {
            return Err(CqlTypeParseError::TypeNotImplemented(id));
        }
    })
}

fn deser_type_borrowed<'frame>(
    buf: &mut &'frame [u8],
) -> StdResult<ColumnType<'frame>, CqlTypeParseError> {
    deser_type_generic(buf, |buf| types::read_string(buf))
}

fn deser_type_owned(buf: &mut &[u8]) -> StdResult<ColumnType<'static>, CqlTypeParseError> {
    deser_type_generic(buf, |buf| types::read_string(buf).map(ToOwned::to_owned))
}

/// Deserializes a table spec, be it per-column one or a global one,
/// in the borrowed form.
///
/// This function does not allocate.
/// To obtain TableSpec<'static>, use `.into_owned()` on its result.
fn deser_table_spec<'frame>(
    buf: &mut &'frame [u8],
) -> StdResult<TableSpec<'frame>, TableSpecParseError> {
    let ks_name = types::read_string(buf).map_err(TableSpecParseError::MalformedKeyspaceName)?;
    let table_name = types::read_string(buf).map_err(TableSpecParseError::MalformedTableName)?;
    Ok(TableSpec::borrowed(ks_name, table_name))
}

fn mk_col_spec_parse_error(
    col_idx: usize,
    err: impl Into<ColumnSpecParseErrorKind>,
) -> ColumnSpecParseError {
    ColumnSpecParseError {
        column_index: col_idx,
        kind: err.into(),
    }
}

fn deser_col_specs_generic<'frame, 'result>(
    buf: &mut &'frame [u8],
    global_table_spec: Option<TableSpec<'frame>>,
    col_count: usize,
    make_col_spec: fn(&'frame str, ColumnType<'result>, TableSpec<'frame>) -> ColumnSpec<'result>,
    deser_type: fn(&mut &'frame [u8]) -> StdResult<ColumnType<'result>, CqlTypeParseError>,
) -> StdResult<Vec<ColumnSpec<'result>>, ColumnSpecParseError> {
    let mut col_specs = Vec::with_capacity(col_count);
    for col_idx in 0..col_count {
        let table_spec = match global_table_spec {
            // If global table spec was provided, we simply clone it to each column spec.
            Some(ref known_spec) => known_spec.clone(),

            // Else, we deserialize the table spec for a column.
            None => deser_table_spec(buf).map_err(|err| mk_col_spec_parse_error(col_idx, err))?,
        };

        let name = types::read_string(buf).map_err(|err| mk_col_spec_parse_error(col_idx, err))?;
        let typ = deser_type(buf).map_err(|err| mk_col_spec_parse_error(col_idx, err))?;
        let col_spec = make_col_spec(name, typ, table_spec);
        col_specs.push(col_spec);
    }
    Ok(col_specs)
}

/// Deserializes col specs (part of ResultMetadata or PreparedMetadata)
/// in the borrowed form.
///
/// To avoid needless allocations, it is advised to pass `global_table_spec`
/// in the borrowed form, so that cloning it is cheap.
fn deser_col_specs_borrowed<'frame>(
    buf: &mut &'frame [u8],
    global_table_spec: Option<TableSpec<'frame>>,
    col_count: usize,
) -> StdResult<Vec<ColumnSpec<'frame>>, ColumnSpecParseError> {
    deser_col_specs_generic(
        buf,
        global_table_spec,
        col_count,
        ColumnSpec::borrowed,
        deser_type_borrowed,
    )
}

/// Deserializes col specs (part of ResultMetadata or PreparedMetadata)
/// in the owned form.
///
/// To avoid needless allocations, it is advised to pass `global_table_spec`
/// in the borrowed form, so that cloning it is cheap.
fn deser_col_specs_owned<'frame>(
    buf: &mut &'frame [u8],
    global_table_spec: Option<TableSpec<'frame>>,
    col_count: usize,
) -> StdResult<Vec<ColumnSpec<'static>>, ColumnSpecParseError> {
    let result: StdResult<Vec<ColumnSpec<'static>>, ColumnSpecParseError> = deser_col_specs_generic(
        buf,
        global_table_spec,
        col_count,
        |name: &str, typ, table_spec: TableSpec| {
            ColumnSpec::owned(name.to_owned(), typ, table_spec.into_owned())
        },
        deser_type_owned,
    );

    result
}

fn deser_result_metadata(
    buf: &mut &[u8],
) -> StdResult<(ResultMetadata<'static>, PagingStateResponse), ResultMetadataParseError> {
    let flags = types::read_int(buf)
        .map_err(|err| ResultMetadataParseError::FlagsParseError(err.into()))?;
    let global_tables_spec = flags & 0x0001 != 0;
    let has_more_pages = flags & 0x0002 != 0;
    let no_metadata = flags & 0x0004 != 0;

    let col_count =
        types::read_int_length(buf).map_err(ResultMetadataParseError::ColumnCountParseError)?;

    let raw_paging_state = has_more_pages
        .then(|| types::read_bytes(buf).map_err(ResultMetadataParseError::PagingStateParseError))
        .transpose()?;

    let paging_state = PagingStateResponse::new_from_raw_bytes(raw_paging_state);

    let col_specs = if no_metadata {
        vec![]
    } else {
        let global_table_spec = global_tables_spec
            .then(|| deser_table_spec(buf))
            .transpose()?;

        deser_col_specs_owned(buf, global_table_spec, col_count)?
    };

    let metadata = ResultMetadata {
        col_count,
        col_specs,
    };
    Ok((metadata, paging_state))
}

impl RawMetadataAndRawRows {
    /// Deserializes flags and paging state; the other part of result metadata
    /// as well as rows remain serialized.
    fn deserialize(
        frame: &mut FrameSlice,
        cached_metadata: Option<Arc<ResultMetadata<'static>>>,
    ) -> StdResult<(Self, PagingStateResponse), RawRowsAndPagingStateResponseParseError> {
        let flags = types::read_int(frame.as_slice_mut())
            .map_err(|err| RawRowsAndPagingStateResponseParseError::FlagsParseError(err.into()))?;
        let global_tables_spec = flags & 0x0001 != 0;
        let has_more_pages = flags & 0x0002 != 0;
        let no_metadata = flags & 0x0004 != 0;

        let col_count = types::read_int_length(frame.as_slice_mut())
            .map_err(RawRowsAndPagingStateResponseParseError::ColumnCountParseError)?;

        let raw_paging_state = has_more_pages
            .then(|| {
                types::read_bytes(frame.as_slice_mut())
                    .map_err(RawRowsAndPagingStateResponseParseError::PagingStateParseError)
            })
            .transpose()?;

        let paging_state = PagingStateResponse::new_from_raw_bytes(raw_paging_state);

        let raw_rows = Self {
            col_count,
            global_tables_spec,
            no_metadata,
            raw_metadata_and_rows: frame.to_bytes(),
            cached_metadata,
        };

        Ok((raw_rows, paging_state))
    }
}

impl RawMetadataAndRawRows {
    // This function is needed because creating the deserializer closure
    // directly in the enclosing function does not provide enough type hints
    // for the compiler (and having a function with a verbose signature does),
    // so it demands a type annotation. We cannot, however, write a correct
    // type annotation, because this way we would limit the lifetime
    // to a concrete lifetime, and our closure needs to be `impl for<'frame> ...`.
    // This is a proud trick by Wojciech Przytuła, which crowns the brilliant
    // idea of Karol Baryła to use Yoke to enable borrowing ResultMetadata
    // from itself.
    fn metadata_deserializer(
        col_count: usize,
        global_tables_spec: bool,
    ) -> impl for<'frame> FnOnce(
        &mut &'frame [u8],
    ) -> StdResult<ResultMetadata<'frame>, ResultMetadataParseError> {
        move |buf| {
            let server_metadata = {
                let global_table_spec = global_tables_spec
                    .then(|| deser_table_spec(buf))
                    .transpose()?;

                let col_specs = deser_col_specs_borrowed(buf, global_table_spec, col_count)?;

                ResultMetadata {
                    col_count,
                    col_specs,
                }
            };
            Ok(server_metadata)
        }
    }

    /// Deserializes ResultMetadata and deserializes rows count. Keeps rows in the serialized form.
    ///
    /// If metadata is cached (in the PreparedStatement), it is reused (shared) from cache
    /// instead of deserializing.
    pub fn deserialize_metadata(
        self,
    ) -> StdResult<DeserializedMetadataAndRawRows, ResultMetadataAndRowsCountParseError> {
        let (metadata_deserialized, row_count_and_raw_rows) = match self.cached_metadata {
            Some(cached) if self.no_metadata => {
                // Server sent no metadata, but we have metadata cached. This means that we asked the server
                // not to send metadata in the response as an optimization. We use cached metadata instead.
                (
                    ResultMetadataHolder::SharedCached(cached),
                    self.raw_metadata_and_rows,
                )
            }
            None if self.no_metadata => {
                // Server sent no metadata and we have no metadata cached. Having no metadata cached,
                // we wouldn't have asked the server for skipping metadata. Therefore, this is most probably
                // not a SELECT, because in such case the server would send empty metadata both in Prepared
                // and in Result responses.
                (
                    ResultMetadataHolder::mock_empty(),
                    self.raw_metadata_and_rows,
                )
            }
            Some(_) | None => {
                // Two possibilities:
                // 1) no cached_metadata provided. Server is supposed to provide the result metadata.
                // 2) cached metadata present (so we should have asked for skipping metadata),
                //    but the server sent result metadata anyway.
                // In case 1 we have to deserialize result metadata. In case 2 we choose to do that,
                // too, because it's suspicious, so we had better use the new metadata just in case.
                // Also, we simply need to advance the buffer pointer past metadata, and this requires
                // parsing metadata.

                let (metadata_container, raw_rows_with_count) =
                    self_borrowed_metadata::SelfBorrowedMetadataContainer::make_deserialized_metadata(
                        self.raw_metadata_and_rows,
                        Self::metadata_deserializer(self.col_count, self.global_tables_spec),
                    )?;
                (
                    ResultMetadataHolder::SelfBorrowed(metadata_container),
                    raw_rows_with_count,
                )
            }
        };

        let mut frame_slice = FrameSlice::new(&row_count_and_raw_rows);

        let rows_count: usize = types::read_int_length(frame_slice.as_slice_mut())
            .map_err(ResultMetadataAndRowsCountParseError::RowsCountParseError)?;

        Ok(DeserializedMetadataAndRawRows {
            metadata: metadata_deserialized,
            rows_count,
            raw_rows: frame_slice.to_bytes(),
        })
    }
}

fn deser_prepared_metadata(
    buf: &mut &[u8],
) -> StdResult<PreparedMetadata, PreparedMetadataParseError> {
    let flags = types::read_int(buf)
        .map_err(|err| PreparedMetadataParseError::FlagsParseError(err.into()))?;
    let global_tables_spec = flags & 0x0001 != 0;

    let col_count =
        types::read_int_length(buf).map_err(PreparedMetadataParseError::ColumnCountParseError)?;

    let pk_count: usize =
        types::read_int_length(buf).map_err(PreparedMetadataParseError::PkCountParseError)?;

    let mut pk_indexes = Vec::with_capacity(pk_count);
    for i in 0..pk_count {
        pk_indexes.push(PartitionKeyIndex {
            index: types::read_short(buf)
                .map_err(|err| PreparedMetadataParseError::PkIndexParseError(err.into()))?
                as u16,
            sequence: i as u16,
        });
    }
    pk_indexes.sort_unstable_by_key(|pki| pki.index);

    let global_table_spec = global_tables_spec
        .then(|| deser_table_spec(buf))
        .transpose()?;

    let col_specs = deser_col_specs_owned(buf, global_table_spec, col_count)?;

    Ok(PreparedMetadata {
        flags,
        col_count,
        pk_indexes,
        col_specs,
    })
}

fn deser_rows(
    buf_bytes: Bytes,
    cached_metadata: Option<&Arc<ResultMetadata<'static>>>,
) -> StdResult<(RawMetadataAndRawRows, PagingStateResponse), RawRowsAndPagingStateResponseParseError>
{
    let mut frame_slice = FrameSlice::new(&buf_bytes);
    RawMetadataAndRawRows::deserialize(&mut frame_slice, cached_metadata.cloned())
}

fn deser_set_keyspace(buf: &mut &[u8]) -> StdResult<SetKeyspace, SetKeyspaceParseError> {
    let keyspace_name = types::read_string(buf)?.to_string();

    Ok(SetKeyspace { keyspace_name })
}

fn deser_prepared(buf: &mut &[u8]) -> StdResult<Prepared, PreparedParseError> {
    let id_len = types::read_short(buf)
        .map_err(|err| PreparedParseError::IdLengthParseError(err.into()))?
        as usize;
    let id: Bytes = buf[0..id_len].to_owned().into();
    buf.advance(id_len);
    let prepared_metadata =
        deser_prepared_metadata(buf).map_err(PreparedParseError::PreparedMetadataParseError)?;
    let (result_metadata, paging_state_response) =
        deser_result_metadata(buf).map_err(PreparedParseError::ResultMetadataParseError)?;
    if let PagingStateResponse::HasMorePages { state } = paging_state_response {
        return Err(PreparedParseError::NonZeroPagingState(
            state
                .as_bytes_slice()
                .cloned()
                .unwrap_or_else(|| Arc::from([])),
        ));
    }

    Ok(Prepared {
        id,
        prepared_metadata,
        result_metadata,
    })
}

fn deser_schema_change(buf: &mut &[u8]) -> StdResult<SchemaChange, SchemaChangeEventParseError> {
    Ok(SchemaChange {
        event: SchemaChangeEvent::deserialize(buf)?,
    })
}

pub fn deserialize(
    buf_bytes: Bytes,
    cached_metadata: Option<&Arc<ResultMetadata<'static>>>,
) -> StdResult<Result, CqlResultParseError> {
    let buf = &mut &*buf_bytes;
    use self::Result::*;
    Ok(
        match types::read_int(buf)
            .map_err(|err| CqlResultParseError::ResultIdParseError(err.into()))?
        {
            0x0001 => Void,
            0x0002 => Rows(deser_rows(buf_bytes.slice_ref(buf), cached_metadata)?),
            0x0003 => SetKeyspace(deser_set_keyspace(buf)?),
            0x0004 => Prepared(deser_prepared(buf)?),
            0x0005 => SchemaChange(deser_schema_change(buf)?),
            id => return Err(CqlResultParseError::UnknownResultId(id)),
        },
    )
}

// This is not #[cfg(test)], because it is used by scylla crate.
// Unfortunately, this attribute does not apply recursively to
// children item. Therefore, every `pub` item here must use have
// the specifier, too.
#[doc(hidden)]
mod test_utils {
    use std::num::TryFromIntError;

    use bytes::{BufMut, BytesMut};

    use super::*;

    impl TableSpec<'_> {
        pub(crate) fn serialize(&self, buf: &mut impl BufMut) -> StdResult<(), TryFromIntError> {
            types::write_string(&self.ks_name, buf)?;
            types::write_string(&self.table_name, buf)?;

            Ok(())
        }
    }

    impl ColumnType<'_> {
        fn id(&self) -> u16 {
            use NativeType::*;
            match self {
                Self::Native(Ascii) => 0x0001,
                Self::Native(BigInt) => 0x0002,
                Self::Native(Blob) => 0x0003,
                Self::Native(Boolean) => 0x0004,
                Self::Native(Counter) => 0x0005,
                Self::Native(Decimal) => 0x0006,
                Self::Native(Double) => 0x0007,
                Self::Native(Float) => 0x0008,
                Self::Native(Int) => 0x0009,
                Self::Native(Timestamp) => 0x000B,
                Self::Native(Uuid) => 0x000C,
                Self::Native(Text) => 0x000D,
                Self::Native(Varint) => 0x000E,
                Self::Native(Timeuuid) => 0x000F,
                Self::Native(Inet) => 0x0010,
                Self::Native(Date) => 0x0011,
                Self::Native(Time) => 0x0012,
                Self::Native(SmallInt) => 0x0013,
                Self::Native(TinyInt) => 0x0014,
                Self::Native(Duration) => 0x0015,
                Self::Collection {
                    typ: CollectionType::List(_),
                    ..
                } => 0x0020,
                Self::Collection {
                    typ: CollectionType::Map(_, _),
                    ..
                } => 0x0021,
                Self::Collection {
                    typ: CollectionType::Set(_),
                    ..
                } => 0x0022,
                Self::Vector { .. } => {
                    unimplemented!();
                }
                Self::UserDefinedType { .. } => 0x0030,
                Self::Tuple(_) => 0x0031,
            }
        }

        // Only for use in tests
        pub(crate) fn serialize(&self, buf: &mut impl BufMut) -> StdResult<(), TryFromIntError> {
            let id = self.id();
            types::write_short(id, buf);

            match self {
                // Simple types
                ColumnType::Native(_) => (),

                ColumnType::Collection {
                    typ: CollectionType::List(elem_type),
                    ..
                }
                | ColumnType::Collection {
                    typ: CollectionType::Set(elem_type),
                    ..
                } => {
                    elem_type.serialize(buf)?;
                }
                ColumnType::Collection {
                    typ: CollectionType::Map(key_type, value_type),
                    ..
                } => {
                    key_type.serialize(buf)?;
                    value_type.serialize(buf)?;
                }
                ColumnType::Tuple(types) => {
                    types::write_short_length(types.len(), buf)?;
                    for typ in types.iter() {
                        typ.serialize(buf)?;
                    }
                }
                ColumnType::Vector { .. } => {
                    unimplemented!()
                }
                ColumnType::UserDefinedType {
                    definition: udt, ..
                } => {
                    types::write_string(&udt.keyspace, buf)?;
                    types::write_string(&udt.name, buf)?;
                    types::write_short_length(udt.field_types.len(), buf)?;
                    for (field_name, field_type) in udt.field_types.iter() {
                        types::write_string(field_name, buf)?;
                        field_type.serialize(buf)?;
                    }
                }
            }

            Ok(())
        }
    }

    impl<'a> ResultMetadata<'a> {
        #[inline]
        #[doc(hidden)]
        pub fn new_for_test(col_count: usize, col_specs: Vec<ColumnSpec<'a>>) -> Self {
            Self {
                col_count,
                col_specs,
            }
        }

        pub(crate) fn serialize(
            &self,
            buf: &mut impl BufMut,
            no_metadata: bool,
            global_tables_spec: bool,
        ) -> StdResult<(), TryFromIntError> {
            let global_table_spec = global_tables_spec
                .then(|| self.col_specs.first().map(|col_spec| col_spec.table_spec()))
                .flatten();

            let mut flags = 0;
            if global_table_spec.is_some() {
                flags |= 0x0001;
            }
            if no_metadata {
                flags |= 0x0004;
            }
            types::write_int(flags, buf);

            types::write_int_length(self.col_count, buf)?;

            // No paging state.

            if !no_metadata {
                if let Some(spec) = global_table_spec {
                    spec.serialize(buf)?;
                }

                for col_spec in self.col_specs() {
                    if global_table_spec.is_none() {
                        col_spec.table_spec().serialize(buf)?;
                    }

                    types::write_string(col_spec.name(), buf)?;
                    col_spec.typ().serialize(buf)?;
                }
            }

            Ok(())
        }
    }

    impl RawMetadataAndRawRows {
        #[doc(hidden)]
        #[inline]
        pub fn new_for_test(
            cached_metadata: Option<Arc<ResultMetadata<'static>>>,
            metadata: Option<ResultMetadata>,
            global_tables_spec: bool,
            rows_count: usize,
            raw_rows: &[u8],
        ) -> StdResult<Self, TryFromIntError> {
            let no_metadata = metadata.is_none();
            let empty_metadata = ResultMetadata::mock_empty();
            let used_metadata = metadata
                .as_ref()
                .or(cached_metadata.as_deref())
                .unwrap_or(&empty_metadata);

            let raw_result_rows = {
                let mut buf = BytesMut::new();
                used_metadata.serialize(&mut buf, global_tables_spec, no_metadata)?;
                types::write_int_length(rows_count, &mut buf)?;
                buf.extend_from_slice(raw_rows);

                buf.freeze()
            };

            let (raw_rows, _paging_state_response) =
                Self::deserialize(&mut FrameSlice::new(&raw_result_rows), cached_metadata).expect(
                    "Ill-formed serialized metadata for tests - likely bug in serialization code",
                );

            Ok(raw_rows)
        }
    }

    impl DeserializedMetadataAndRawRows {
        #[inline]
        #[cfg(test)]
        pub(crate) fn new_for_test(
            metadata: ResultMetadata<'static>,
            rows_count: usize,
            raw_rows: Bytes,
        ) -> Self {
            Self {
                metadata: ResultMetadataHolder::SharedCached(Arc::new(metadata)),
                rows_count,
                raw_rows,
            }
        }
    }
}
