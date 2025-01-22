use super::result::CollectionType;
use super::result::NativeType;
use super::result::{ColumnType, UserDefinedType};
use crate::frame::frame_errors::CustomTypeParseError;
use crate::utils::parse::ParserState;
use either;
use std::borrow::Cow;
use std::char;
use std::str::from_utf8;
use std::sync::Arc;

struct UDTParameters<'result> {
    type_name: Cow<'result, str>,
    keyspace: Cow<'result, str>,
    field_types: Vec<(Cow<'result, str>, ColumnType<'result>)>,
}

pub(crate) struct TypeParser<'result> {
    parser: ParserState<'result>,
}

impl<'result> TypeParser<'result> {
    fn new(input: &'result str) -> TypeParser<'result> {
        Self {
            parser: ParserState::new(input),
        }
    }

    pub(crate) fn parse(input: &'result str) -> Result<ColumnType<'result>, CustomTypeParseError> {
        let mut parser = TypeParser::new(input);
        parser.do_parse()
    }

    fn is_identifier_char(c: char) -> bool {
        c.is_alphanumeric() || c == '+' || c == '-' || c == '_' || c == '.' || c == '&'
    }

    fn read_next_identifier(&mut self) -> &'result str {
        let (res, parser) = self.parser.take_while(TypeParser::is_identifier_char);
        self.parser = parser;
        res
    }

    fn skip_blank(&mut self) {
        self.parser = self.parser.skip_white()
    }

    fn skip_blank_and_comma(&mut self) {
        let mut comma_found = false;
        loop {
            self.skip_blank();
            // If we haven't already skipped a comma, we check if we can skip a comma and mark it.
            if !comma_found {
                let result = self.parser.accept(",");
                match result {
                    Ok(parser) => {
                        comma_found = true;
                        self.parser = parser;
                    }
                    Err(_) => {
                        return;
                    }
                }
            } else {
                return;
            }
        }
    }

    fn get_simple_abstract_type(
        mut name: &'result str,
    ) -> Result<ColumnType<'result>, CustomTypeParseError> {
        name = name
            .strip_prefix("org.apache.cassandra.db.marshal.")
            .unwrap_or(name);

        match name {
            "AsciiType" => Ok(ColumnType::Native(NativeType::Ascii)),
            "BooleanType" => Ok(ColumnType::Native(NativeType::Boolean)),
            "BytesType" => Ok(ColumnType::Native(NativeType::Blob)),
            "CounterColumnType" => Ok(ColumnType::Native(NativeType::Counter)),
            "DateType" => Ok(ColumnType::Native(NativeType::Date)),
            "DecimalType" => Ok(ColumnType::Native(NativeType::Decimal)),
            "DoubleType" => Ok(ColumnType::Native(NativeType::Double)),
            "DurationType" => Ok(ColumnType::Native(NativeType::Duration)),
            "FloatType" => Ok(ColumnType::Native(NativeType::Float)),
            "InetAddressType" => Ok(ColumnType::Native(NativeType::Inet)),
            "Int32Type" => Ok(ColumnType::Native(NativeType::Int)),
            "IntegerType" => Ok(ColumnType::Native(NativeType::Varint)),
            "LongType" => Ok(ColumnType::Native(NativeType::BigInt)),
            "SimpleDateType" => Ok(ColumnType::Native(NativeType::Date)),
            "ShortType" => Ok(ColumnType::Native(NativeType::SmallInt)),
            "UTF8Type" => Ok(ColumnType::Native(NativeType::Text)),
            "ByteType" => Ok(ColumnType::Native(NativeType::TinyInt)),
            "UUIDType" => Ok(ColumnType::Native(NativeType::Uuid)),
            "TimeUUIDType" => Ok(ColumnType::Native(NativeType::Timeuuid)),
            "SmallIntType" => Ok(ColumnType::Native(NativeType::SmallInt)),
            "TinyIntType" => Ok(ColumnType::Native(NativeType::TinyInt)),
            "TimeType" => Ok(ColumnType::Native(NativeType::Time)),
            "TimestampType" => Ok(ColumnType::Native(NativeType::Timestamp)),
            _ => Err(CustomTypeParseError::UnknownSimpleCustomTypeName(
                name.into(),
            )),
        }
    }

    fn get_type_parameters(
        &mut self,
    ) -> Result<
        impl Iterator<Item = Result<ColumnType<'result>, CustomTypeParseError>> + '_,
        CustomTypeParseError,
    > {
        if self.parser.is_at_eof() {
            return Ok(either::Left(std::iter::empty()));
        }
        self.parser = self
            .parser
            .accept("(")
            .map_err(|_| CustomTypeParseError::BadCharacter)?;

        Ok(either::Right(std::iter::from_fn(|| {
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
        self.parser = self
            .parser
            .accept("(")
            .map_err(|_| CustomTypeParseError::BadCharacter)?;

        self.skip_blank_and_comma();
        if self.parser.accept(")").is_ok() {
            return Err(CustomTypeParseError::BadCharacter);
        }

        let typ = self.do_parse()?;
        self.skip_blank_and_comma();
        let (len, parser) = self
            .parser
            .parse_u16()
            .map_err(|_| CustomTypeParseError::BadCharacter)?;
        self.parser = parser;
        self.parser = self
            .parser
            .accept(")")
            .map_err(|_| CustomTypeParseError::BadCharacter)?;
        Ok((typ, len))
    }

    /// Parse a string of hexadecimal representation of bytes into a byte vector.
    fn from_hex(s: &'result str) -> Result<Vec<u8>, CustomTypeParseError> {
        if s.len() % 2 != 0 {
            return Err(CustomTypeParseError::BadHexString);
        }
        for c in s.chars() {
            if !c.is_ascii_hexdigit() {
                return Err(CustomTypeParseError::BadHexString);
            }
        }
        let mut bytes = Vec::new();
        for i in 0..s.len() / 2 {
            let byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
                .map_err(|_| CustomTypeParseError::BadHexString)?;
            bytes.push(byte);
        }
        Ok(bytes)
    }

    fn get_udt_parameters(&mut self) -> Result<UDTParameters<'result>, CustomTypeParseError> {
        self.parser = self
            .parser
            .accept("(")
            .map_err(|_| CustomTypeParseError::BadCharacter)?;

        self.skip_blank_and_comma();
        let keyspace = self.read_next_identifier();
        self.skip_blank_and_comma();
        let hex_name = &TypeParser::from_hex(self.read_next_identifier())?;
        let name = from_utf8(hex_name).map_err(|_| CustomTypeParseError::InvalidUtf8)?;
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
                        keyspace: Cow::Borrowed(keyspace),
                        type_name: Cow::Owned(name.to_owned()),
                        field_types: fields,
                    });
                }
                Err(_) => {
                    let field_hex = &TypeParser::from_hex(self.read_next_identifier())?;
                    let field_name =
                        from_utf8(field_hex).map_err(|_| CustomTypeParseError::InvalidUtf8)?;
                    self.parser = self
                        .parser
                        .accept(":")
                        .map_err(|_| CustomTypeParseError::BadCharacter)?;
                    let field_type = self.do_parse()?;
                    fields.push((Cow::Owned(field_name.into()), field_type));
                }
            }
        }
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
                let mut params = self.get_type_parameters()?;
                let param1 = params
                    .next()
                    .ok_or(CustomTypeParseError::InvalidParameterCount)??;
                let param2 = params.next();
                if param2.is_some() {
                    return Err(CustomTypeParseError::InvalidParameterCount);
                }
                Ok(ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::List(Box::new(param1)),
                })
            }
            "SetType" => {
                let mut params = self.get_type_parameters()?;
                let param1 = params
                    .next()
                    .ok_or(CustomTypeParseError::InvalidParameterCount)??;
                let param2 = params.next();
                if param2.is_some() {
                    return Err(CustomTypeParseError::InvalidParameterCount);
                }
                Ok(ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::Set(Box::new(param1)),
                })
            }
            "MapType" => {
                let mut params = self.get_type_parameters()?;
                let param1 = params
                    .next()
                    .ok_or(CustomTypeParseError::InvalidParameterCount)??;
                let param2 = params
                    .next()
                    .ok_or(CustomTypeParseError::InvalidParameterCount)??;
                let param3 = params.next();
                if param3.is_some() {
                    return Err(CustomTypeParseError::InvalidParameterCount);
                }
                Ok(ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::Map(Box::new(param1), Box::new(param2)),
                })
            }
            "TupleType" => {
                let params = self
                    .get_type_parameters()?
                    .collect::<Result<Vec<_>, CustomTypeParseError>>()?;
                if params.is_empty() {
                    return Err(CustomTypeParseError::InvalidParameterCount);
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
                        name: params.type_name,
                        keyspace: params.keyspace,
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
        if name.is_empty() {
            if !self.parser.is_at_eof() {
                return Err(CustomTypeParseError::UnknownComplexCustomTypeName(
                    self.parser.s.into(),
                ));
            }
            return Ok(ColumnType::Native(NativeType::Blob));
        }

        // Type can be prefixed by a hex number, which is ignored.
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
            Err(_) => TypeParser::get_simple_abstract_type(name),
        }
    }
}
