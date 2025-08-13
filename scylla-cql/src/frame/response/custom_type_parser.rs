//! Implementation of a parser for custom types in the CQL protocol.

use super::result::CollectionType;
use super::result::NativeType;
use super::result::{ColumnType, UserDefinedType};
use crate::frame::frame_errors::CustomTypeParseError;
use crate::utils::parse::ParseResult;
use crate::utils::parse::ParserState;
use itertools::Either;
use itertools::Itertools as _;
use std::borrow::Cow;
use std::char;
use std::sync::Arc;

// Here the first parameter is a String, as it is converted from a hex byte array,
// so it will always be owned.
// The second parameter is read by the parser, it will always be borrowed.
// The first two parameters are trivially convertible to Cow, which is the type used in the UDT struct,
// however, a vector of strings would not be trivially convertible, so we put the Cow type here.
struct UDTParameters<'result> {
    type_name: String,
    keyspace: &'result str,
    field_types: Vec<(Cow<'result, str>, ColumnType<'result>)>,
}

// This parser is used to parse the type names in the CQL protocol.
// The logic is based on the ScyllaDB implementation.
// https://github.com/scylladb/scylladb/blob/f5125ffa18871ae74c08a4337c599009d5dd48a9/db/marshal/type_parser.cc

pub(crate) struct CustomTypeParser<'result> {
    parser: ParserState<'result>,
}

impl<'result> CustomTypeParser<'result> {
    fn new(input: &'result str) -> CustomTypeParser<'result> {
        Self {
            parser: ParserState::new(input),
        }
    }

    fn get_first_char(&self) -> char {
        self.parser.s.chars().next().unwrap_or('\0')
    }

    fn accept_in_place(&mut self, s: &'static str) -> ParseResult<()> {
        self.parser = self.parser.accept(s)?;
        Ok(())
    }

    pub(crate) fn parse(input: &'result str) -> Result<ColumnType<'result>, CustomTypeParseError> {
        let mut parser = CustomTypeParser::new(input);
        parser.do_parse()
    }

    fn is_identifier_char(c: char) -> bool {
        c.is_alphanumeric() || c == '+' || c == '-' || c == '_' || c == '.' || c == '&'
    }

    fn read_next_identifier(&mut self) -> &'result str {
        let (res, parser) = self.parser.take_while(CustomTypeParser::is_identifier_char);
        self.parser = parser;
        res
    }

    fn skip_blank(&mut self) {
        self.parser = self.parser.skip_white()
    }

    // This functions skips blank spaces and, optionally, one comma.
    // This behavior is consistent with the ScyllaDB implementation.
    // See: https://github.com/scylladb/scylladb/blob/f5125ffa18871ae74c08a4337c599009d5dd48a9/db/marshal/type_parser.cc#L290
    fn skip_blank_and_comma(&mut self) {
        self.skip_blank();

        if let Ok(parser) = self.parser.accept(",") {
            self.parser = parser;
            self.skip_blank();
        }
    }

    fn get_simple_abstract_type(
        mut name: &'result str,
    ) -> Result<ColumnType<'result>, CustomTypeParseError> {
        name = name
            .strip_prefix("org.apache.cassandra.db.marshal.")
            .unwrap_or(name);

        let native = match name {
            "AsciiType" => NativeType::Ascii,
            "BooleanType" => NativeType::Boolean,
            "BytesType" => NativeType::Blob,
            "CounterColumnType" => NativeType::Counter,
            "DateType" => NativeType::Date,
            "DecimalType" => NativeType::Decimal,
            "DoubleType" => NativeType::Double,
            "DurationType" => NativeType::Duration,
            "FloatType" => NativeType::Float,
            "InetAddressType" => NativeType::Inet,
            "Int32Type" => NativeType::Int,
            "IntegerType" => NativeType::Varint,
            "LongType" => NativeType::BigInt,
            "SimpleDateType" => NativeType::Date,
            "ShortType" => NativeType::SmallInt,
            "UTF8Type" => NativeType::Text,
            "ByteType" => NativeType::TinyInt,
            "UUIDType" => NativeType::Uuid,
            "TimeUUIDType" => NativeType::Timeuuid,
            "SmallIntType" => NativeType::SmallInt,
            "TinyIntType" => NativeType::TinyInt,
            "TimeType" => NativeType::Time,
            "TimestampType" => NativeType::Timestamp,
            _ => {
                return Err(CustomTypeParseError::UnknownSimpleCustomTypeName(
                    name.into(),
                ));
            }
        };
        Ok(ColumnType::Native(native))
    }

    fn get_type_parameters(
        &mut self,
    ) -> Result<
        impl Iterator<Item = Result<ColumnType<'result>, CustomTypeParseError>> + '_,
        CustomTypeParseError,
    > {
        if self.parser.is_at_eof() {
            return Ok(Either::Left(std::iter::empty()));
        }
        self.accept_in_place("(")
            .map_err(|_| CustomTypeParseError::UnexpectedCharacter(self.get_first_char(), '('))?;

        Ok(Either::Right(std::iter::from_fn(|| {
            self.skip_blank_and_comma();
            if self.parser.is_at_eof() {
                return Some(Err(CustomTypeParseError::UnexpectedEndOfInput));
            }
            let result = self.parser.accept(")");
            match result {
                Ok(parser) => {
                    self.parser = parser;
                    None
                }
                Err(_) => Some(self.do_parse()),
            }
        })))
    }

    fn get_vector_parameters(
        &mut self,
    ) -> Result<(ColumnType<'result>, u16), CustomTypeParseError> {
        self.accept_in_place("(")
            .map_err(|_| CustomTypeParseError::UnexpectedCharacter(self.get_first_char(), '('))?;

        self.skip_blank_and_comma();
        // This happens when no parameters are provided.
        // See: https://github.com/scylladb/scylladb/blob/f5125ffa18871ae74c08a4337c599009d5dd48a9/db/marshal/type_parser.cc#L290
        if self.parser.accept(")").is_ok() {
            return Err(CustomTypeParseError::InvalidParameterCount {
                actual: 0,
                expected: 2,
            });
        }

        let typ = self.do_parse()?;
        self.skip_blank_and_comma();
        let (len, parser) = self
            .parser
            .parse_u16()
            .map_err(|err| CustomTypeParseError::IntegerParseError(err.get_cause()))?;
        self.parser = parser;
        self.accept_in_place(")")
            .map_err(|_| CustomTypeParseError::UnexpectedCharacter(self.get_first_char(), ')'))?;
        Ok((typ, len))
    }

    /// Parse a string of hexadecimal representation of bytes into a byte vector.
    fn from_hex(s: &'result str) -> Result<Vec<u8>, CustomTypeParseError> {
        if s.len() % 2 != 0 {
            return Err(CustomTypeParseError::BadHexString(s.to_owned()));
        }
        for c in s.chars() {
            if !c.is_ascii_hexdigit() {
                return Err(CustomTypeParseError::BadHexString(s.to_owned()));
            }
        }
        let bytes: Vec<_> = s
            .as_bytes()
            .chunks_exact(2)
            .map(|chunk| {
                // Safety: All characters were checked to be valid ASCII hexdigits,
                // so two-byte chunks are valid ASCII strings, too.
                u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16)
            })
            .collect::<Result<_, _>>()
            .map_err(|_| CustomTypeParseError::BadHexString(s.to_owned()))?;
        Ok(bytes)
    }

    fn get_udt_parameters(&mut self) -> Result<UDTParameters<'result>, CustomTypeParseError> {
        self.accept_in_place("(")
            .map_err(|_| CustomTypeParseError::UnexpectedCharacter(self.get_first_char(), '('))?;

        self.skip_blank_and_comma();
        let keyspace = self.read_next_identifier();
        self.skip_blank_and_comma();
        let hex_name = CustomTypeParser::from_hex(self.read_next_identifier())?;
        let name = String::from_utf8(hex_name)
            .map_err(|err| CustomTypeParseError::InvalidUtf8(err.into_bytes()))?;
        let mut fields = Vec::new();
        loop {
            self.skip_blank_and_comma();
            if self.parser.is_at_eof() {
                return Err(CustomTypeParseError::UnexpectedEndOfInput);
            }
            let result = self.parser.accept(")");
            match result {
                Ok(parser) => {
                    self.parser = parser;
                    return Ok(UDTParameters {
                        keyspace,
                        type_name: name,
                        field_types: fields,
                    });
                }
                Err(_) => {
                    let field_hex = CustomTypeParser::from_hex(self.read_next_identifier())?;
                    let field_name = String::from_utf8(field_hex)
                        .map_err(|err| CustomTypeParseError::InvalidUtf8(err.into_bytes()))?;
                    self.accept_in_place(":").map_err(|_| {
                        CustomTypeParseError::UnexpectedCharacter(self.get_first_char(), ':')
                    })?;
                    let field_type = self.do_parse()?;
                    fields.push((Cow::Owned(field_name), field_type));
                }
            }
        }
    }

    fn get_n_type_parameters<const N: usize>(
        &mut self,
    ) -> Result<[Result<ColumnType<'result>, CustomTypeParseError>; N], CustomTypeParseError> {
        let mut backup = Self {
            parser: self.parser,
        };

        self.get_type_parameters()?
            .collect_array::<N>()
            .ok_or_else(|| {
                // unwrap(): get_type_parameters() already worked above, so it will work here as well.

                let actual_parameter_count = backup.get_type_parameters().unwrap().count();

                CustomTypeParseError::InvalidParameterCount {
                    actual: actual_parameter_count,
                    expected: N,
                }
            })
    }

    fn get_complex_abstract_type(
        &mut self,
        mut name: &'result str,
    ) -> Result<ColumnType<'result>, CustomTypeParseError> {
        name = name
            .strip_prefix("org.apache.cassandra.db.marshal.")
            .unwrap_or(name);

        match name {
            "ListType" => {
                let [element_type_result] = self.get_n_type_parameters::<1>()?;
                let element_type = element_type_result?;
                Ok(ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::List(Box::new(element_type)),
                })
            }
            "SetType" => {
                let [element_type_result] = self.get_n_type_parameters::<1>()?;
                let element_type = element_type_result?;
                Ok(ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::Set(Box::new(element_type)),
                })
            }
            "MapType" => {
                let [key_type_result, value_type_result] = self.get_n_type_parameters::<2>()?;
                let key_type = key_type_result?;
                let value_type = value_type_result?;
                Ok(ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::Map(Box::new(key_type), Box::new(value_type)),
                })
            }
            "TupleType" => {
                let params = self
                    .get_type_parameters()?
                    .collect::<Result<Vec<_>, CustomTypeParseError>>()?;
                if params.is_empty() {
                    return Err(CustomTypeParseError::InvalidParameterCount {
                        actual: 0,
                        expected: 1,
                    });
                }
                Ok(ColumnType::Tuple(params))
            }
            "VectorType" => {
                let (typ, len) = self.get_vector_parameters()?;
                Ok(ColumnType::Vector {
                    typ: Box::new(typ),
                    dimensions: len,
                })
            }
            "UserType" => {
                let params = self.get_udt_parameters()?;
                Ok(ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: params.type_name.into(),
                        keyspace: params.keyspace.into(),
                        field_types: params.field_types,
                    }),
                })
            }
            name => Err(CustomTypeParseError::UnknownComplexCustomTypeName(
                name.into(),
            )),
        }
    }

    fn do_parse(&mut self) -> Result<ColumnType<'result>, CustomTypeParseError> {
        self.skip_blank();

        let mut name = self.read_next_identifier();

        // Not sure why do we return blob, however this code is ripped from ScyllaDB codebase and they do it this way.
        // See: https://github.com/scylladb/scylladb/commit/f9b83e299e0c8f6bb5b8084afd6eb6c84e5a9b24
        if name.is_empty() {
            if !self.parser.is_at_eof() {
                return Err(CustomTypeParseError::UnknownComplexCustomTypeName(
                    self.parser.s.into(),
                ));
            }
            return Ok(ColumnType::Native(NativeType::Blob));
        }

        // Type can be prefixed by a hex number, which is ignored.
        // See the reasoning here: https://github.com/scylladb/scylladb/commit/85be9f55e8d7692ef4d17458d21049e565ba3680
        let result = self.parser.accept(":");
        if let Ok(parser) = result {
            self.parser = parser;
            let _ = usize::from_str_radix(name, 16).map_err(|_| CustomTypeParseError::BadHexString);
            name = self.read_next_identifier();
        }
        self.skip_blank();
        let result = self.parser.accept("(");
        match result {
            // Here we do not change the parser state, because we want to keep the state as it was before the accept.
            Ok(_) => self.get_complex_abstract_type(name),
            Err(_) => CustomTypeParser::get_simple_abstract_type(name),
        }
    }
}
