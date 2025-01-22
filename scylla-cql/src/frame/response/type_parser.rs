use crate::frame::frame_errors::CustomTypeParseError;
use std::sync::Arc;
use std::{borrow::Cow, char, str::from_utf8};

use super::result::CollectionType;
use super::result::NativeType;
use super::result::{ColumnType, UserDefinedType};

type UDTParameters<'result> = (
    Cow<'result, str>,
    Cow<'result, str>,
    Vec<(Cow<'result, str>, ColumnType<'result>)>,
);

pub(crate) struct TypeParser<'result> {
    pos: usize,
    parsed_string: Cow<'result, str>,
}

impl<'result> TypeParser<'result> {
        TypeParser {
    }

    pub(crate) fn parse(
        str: Cow<'result, str>,
    ) -> Result<ColumnType<'result>, CustomTypeParseError> {
        let mut parser = TypeParser::new(str);
        parser.do_parse()
    }

    fn is_blank(c: char) -> bool {
        c == ' ' || c == '\t' || c == '\n'
    }

    fn is_identifier_char(c: char) -> bool {
        c.is_alphanumeric() || c == '+' || c == '-' || c == '_' || c == '.' || c == '&'
    }

            .as_bytes()
    }

    fn read_next_identifier(&mut self) -> Cow<'result, str> {
        let start = self.pos;
        loop {
            match self.char_at_pos() {
                Some(c) if TypeParser::is_identifier_char(c) => {
                    self.pos += 1;
                }
                _ => {
                    break;
                }
            }
        }
        match &self.parsed_string {
            Cow::Borrowed(s) => Cow::Borrowed(&s[start..self.pos]),
            Cow::Owned(s) => Cow::Owned(s[start..self.pos].to_owned()),
        }
    }

    fn skip_blank(&mut self) -> usize {
        loop {
            match self.char_at_pos() {
                Some(c) if TypeParser::is_blank(c) => {
                    self.pos += 1;
                }
                _ => {
                    break;
                }
            }
        }
        self.pos
    }

    // Returns true if a we did not reach the end of the input
    fn skip_blank_and_comma(&mut self) -> bool {
        let mut comma_found = false;
        loop {
            match self.char_at_pos() {
                Some(',') => {
                    if comma_found {
                        return true;
                    } else {
                        comma_found = true;
                    }
                }
                Some(c) if !TypeParser::is_blank(c) => {
                    return true;
                }
                None => {
                    break;
                }
                _ => {}
            }
            self.pos += 1;
        }
        false
    }

    fn get_simple_abstract_type(
        name: Cow<'result, str>,
    ) -> Result<ColumnType<'result>, CustomTypeParseError> {
        let string_class_name: String;
        let class_name: Cow<'result, str>;
        if name.contains("org.apache.cassandra.db.marshal.") {
            class_name = name;
        } else {
            string_class_name = "org.apache.cassandra.db.marshal.".to_owned() + &name;
            class_name = Cow::Owned(string_class_name);
        }

        match class_name.as_ref() {
            "org.apache.cassandra.db.marshal.AsciiType" => {
                Ok(ColumnType::Native(NativeType::Ascii))
            }
            "org.apache.cassandra.db.marshal.BooleanType" => {
                Ok(ColumnType::Native(NativeType::Boolean))
            }
            "org.apache.cassandra.db.marshal.BytesType" => Ok(ColumnType::Native(NativeType::Blob)),
            "org.apache.cassandra.db.marshal.CounterColumnType" => {
                Ok(ColumnType::Native(NativeType::Counter))
            }
            "org.apache.cassandra.db.marshal.DateType" => Ok(ColumnType::Native(NativeType::Date)),
            "org.apache.cassandra.db.marshal.DecimalType" => {
                Ok(ColumnType::Native(NativeType::Decimal))
            }
            "org.apache.cassandra.db.marshal.DoubleType" => {
                Ok(ColumnType::Native(NativeType::Double))
            }
            "org.apache.cassandra.db.marshal.DurationType" => {
                Ok(ColumnType::Native(NativeType::Duration))
            }
            "org.apache.cassandra.db.marshal.FloatType" => {
                Ok(ColumnType::Native(NativeType::Float))
            }
            "org.apache.cassandra.db.marshal.InetAddressType" => {
                Ok(ColumnType::Native(NativeType::Inet))
            }
            "org.apache.cassandra.db.marshal.Int32Type" => Ok(ColumnType::Native(NativeType::Int)),
            "org.apache.cassandra.db.marshal.IntegerType" => {
                Ok(ColumnType::Native(NativeType::Varint))
            }
            "org.apache.cassandra.db.marshal.LongType" => {
                Ok(ColumnType::Native(NativeType::BigInt))
            }
            "org.apache.cassandra.db.marshal.SimpleDateType" => {
                Ok(ColumnType::Native(NativeType::Date))
            }
            "org.apache.cassandra.db.marshal.ShortType" => {
                Ok(ColumnType::Native(NativeType::SmallInt))
            }
            "org.apache.cassandra.db.marshal.UTF8Type" => Ok(ColumnType::Native(NativeType::Text)),
            "org.apache.cassandra.db.marshal.ByteType" => {
                Ok(ColumnType::Native(NativeType::TinyInt))
            }
            "org.apache.cassandra.db.marshal.UUIDType" => Ok(ColumnType::Native(NativeType::Uuid)),
            "org.apache.cassandra.db.marshal.TimeUUIDType" => {
                Ok(ColumnType::Native(NativeType::Timeuuid))
            }
            "org.apache.cassandra.db.marshal.SmallIntType" => {
                Ok(ColumnType::Native(NativeType::SmallInt))
            }
            "org.apache.cassandra.db.marshal.TinyIntType" => {
                Ok(ColumnType::Native(NativeType::TinyInt))
            }
            "org.apache.cassandra.db.marshal.TimeType" => Ok(ColumnType::Native(NativeType::Time)),
            "org.apache.cassandra.db.marshal.TimestampType" => {
                Ok(ColumnType::Native(NativeType::Timestamp))
            }
            _ => Err(CustomTypeParseError::UnknownSimpleCustomTypeName(
                class_name.into(),
            )),
        }
    }

    fn get_type_parameters(&mut self) -> Result<Vec<ColumnType<'result>>, CustomTypeParseError> {
        let mut parameters = Vec::new();
        match self.char_at_pos() {
            Some('(') => {}
            None => {
                return Ok(parameters);
            }
            _ => {
                return Err(CustomTypeParseError::BadCharacter);
            }
        }
        self.pos += 1;
        loop {
            if !self.skip_blank_and_comma() {
                return Err(CustomTypeParseError::UnexpectedEndOfInput);
            }
            if let Some(')') = self.char_at_pos() {
                self.pos += 1;
                return Ok(parameters);
            }
            let typ = self.do_parse()?;
            parameters.push(typ);
        }
    }

    fn from_hex(s: Cow<'result, str>) -> Result<Vec<u8>, CustomTypeParseError> {
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
        match self.char_at_pos() {
            Some('(') => {}
            _ => {
                return Err(CustomTypeParseError::BadCharacter);
            }
        }

        self.pos += 1;

        self.skip_blank_and_comma();
        let keyspace = self.read_next_identifier();
        self.skip_blank_and_comma();
        let hex_name = &TypeParser::from_hex(self.read_next_identifier())?;
        let name = from_utf8(hex_name).map_err(|_| CustomTypeParseError::InvalidUtf8)?;
        let mut fields = Vec::new();
        loop {
            if !self.skip_blank_and_comma() {
                return Err(CustomTypeParseError::UnexpectedEndOfInput);
            }
            if let Some(')') = self.char_at_pos() {
                self.pos += 1;
                return Ok((keyspace, Cow::Owned(name.to_owned()), fields));
            }
            let field_name = self.read_next_identifier();
            self.skip_blank_and_comma();
            let field_type = self.do_parse()?;
            fields.push((field_name, field_type));
        }
    }

    fn get_complex_abstract_type(
        &mut self,
        name: Cow<'result, str>,
    ) -> Result<ColumnType<'result>, CustomTypeParseError> {
        let string_class_name: String;
        let class_name: Cow<'result, str>;
        if name.contains("org.apache.cassandra.db.marshal.") {
            class_name = name
        } else {
            string_class_name = "org.apache.cassandra.db.marshal.".to_owned() + &name;
            class_name = Cow::Owned(string_class_name);
        }
        match class_name.as_ref() {
            "org.apache.cassandra.db.marshal.ListType" => {
                let mut params = self.get_type_parameters()?;
                if params.len() != 1 {
                    return Err(CustomTypeParseError::InvalidParameterCount);
                }
                Ok(ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::List(Box::new(params.remove(0))),
                })
            }
            "org.apache.cassandra.db.marshal.SetType" => {
                let mut params = self.get_type_parameters()?;
                if params.len() != 1 {
                    return Err(CustomTypeParseError::InvalidParameterCount);
                }
                Ok(ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::Set(Box::new(params.remove(0))),
                })
            }
            "org.apache.cassandra.db.marshal.MapType" => {
                let mut params = self.get_type_parameters()?;
                if params.len() != 2 {
                    return Err(CustomTypeParseError::InvalidParameterCount);
                }
                Ok(ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::Map(
                        Box::new(params.remove(0)),
                        Box::new(params.remove(0)),
                    ),
                })
            }
            "org.apache.cassandra.db.marshal.TupleType" => {
                let params = self.get_type_parameters()?;
                if params.is_empty() {
                    return Err(CustomTypeParseError::InvalidParameterCount);
                }
                Ok(ColumnType::Tuple(params))
            }
            "org.apache.cassandra.db.marshal.UserType" => {
                let (keyspace, name, fields) = self.get_udt_parameters()?;
                Ok(ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name,
                        keyspace,
                        field_types: fields,
                    }),
                })
            }
            _ => Err(CustomTypeParseError::UnknownComplexCustomTypeName(
                class_name.into(),
            )),
        }
    }

    fn do_parse(&mut self) -> Result<ColumnType<'result>, CustomTypeParseError> {
        self.skip_blank();

        let mut name = self.read_next_identifier();
        if name.is_empty() {
            match self.char_at_pos() {
                Some(_) => {
                    return Err(CustomTypeParseError::UnknownParserError);
                }
                None => {
                    return Ok(ColumnType::Native(NativeType::Blob));
                }
            }
        }

        if let Some(':') = self.char_at_pos() {
            self.pos += 1;
            let _ =
                usize::from_str_radix(&name, 16).map_err(|_| CustomTypeParseError::BadHexString);
            name = self.read_next_identifier();
        }
        self.skip_blank();
        match self.char_at_pos() {
            Some('(') => self.get_complex_abstract_type(name),
            _ => TypeParser::get_simple_abstract_type(name),
        }
    }
}
