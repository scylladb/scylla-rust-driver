use crate::frame::frame_errors::CqlTypeParseError;
use std::{borrow::Cow, char, str::from_utf8};

use super::result::ColumnType;

type UDTParameters<'result> = (
    Cow<'result, str>,
    Cow<'result, str>,
    Vec<(Cow<'result, str>, ColumnType<'result>)>,
);

pub(crate) struct TypeParser<'result> {
    pos: usize,
    str: Cow<'result, str>,
}

impl<'result> TypeParser<'result> {
    fn new(str: Cow<'result, str>) -> TypeParser<'result> {
        TypeParser { pos: 0, str }
    }

    pub(crate) fn parse(str: Cow<'result, str>) -> Result<ColumnType<'result>, CqlTypeParseError> {
        let mut parser = TypeParser::new(str);
        parser.do_parse()
    }

    fn is_eos(&self) -> bool {
        self.pos >= self.str.len()
    }

    fn is_blank(c: char) -> bool {
        c == ' ' || c == '\t' || c == '\n'
    }

    fn is_identifier_char(c: char) -> bool {
        c.is_alphanumeric() || c == '+' || c == '-' || c == '_' || c == '.' || c == '&'
    }

    fn char_at_pos(&self) -> char {
        self.str.as_bytes()[self.pos] as char
    }

    fn read_next_identifier(&mut self) -> Cow<'result, str> {
        let start = self.pos;
        while !self.is_eos() && TypeParser::is_identifier_char(self.char_at_pos()) {
            self.pos += 1;
        }
        match &self.str {
            Cow::Borrowed(s) => Cow::Borrowed(&s[start..self.pos]),
            Cow::Owned(s) => Cow::Owned(s[start..self.pos].to_owned()),
        }
    }

    fn skip_blank(&mut self) -> usize {
        while !self.is_eos() && TypeParser::is_blank(self.char_at_pos()) {
            self.pos += 1;
        }
        self.pos
    }

    fn skip_blank_and_comma(&mut self) -> bool {
        let mut comma_found = false;
        while !self.is_eos() {
            let c = self.char_at_pos();
            if c == ',' {
                if comma_found {
                    return true;
                } else {
                    comma_found = true;
                }
            } else if !TypeParser::is_blank(c) {
                return true;
            }
            self.pos += 1;
        }
        false
    }

    fn get_simple_abstract_type(
        name: Cow<'result, str>,
    ) -> Result<ColumnType<'result>, CqlTypeParseError> {
        let string_class_name: String;
        let class_name: Cow<'result, str>;
        if name.contains("org.apache.cassandra.db.marshal.") {
            class_name = name
        } else {
            string_class_name = "org.apache.cassandra.db.marshal.".to_owned() + &name;
            class_name = Cow::Owned(string_class_name);
        }

        match class_name.as_ref() {
            "org.apache.cassandra.db.marshal.AsciiType" => Ok(ColumnType::Ascii),
            "org.apache.cassandra.db.marshal.BooleanType" => Ok(ColumnType::Boolean),
            "org.apache.cassandra.db.marshal.BytesType" => Ok(ColumnType::Blob),
            "org.apache.cassandra.db.marshal.CounterColumnType" => Ok(ColumnType::Counter),
            "org.apache.cassandra.db.marshal.DateType" => Ok(ColumnType::Date),
            "org.apache.cassandra.db.marshal.DecimalType" => Ok(ColumnType::Decimal),
            "org.apache.cassandra.db.marshal.DoubleType" => Ok(ColumnType::Double),
            "org.apache.cassandra.db.marshal.DurationType" => Ok(ColumnType::Duration),
            "org.apache.cassandra.db.marshal.FloatType" => Ok(ColumnType::Float),
            "org.apache.cassandra.db.marshal.InetAddressType" => Ok(ColumnType::Inet),
            "org.apache.cassandra.db.marshal.Int32Type" => Ok(ColumnType::Int),
            "org.apache.cassandra.db.marshal.IntegerType" => Ok(ColumnType::Varint),
            "org.apache.cassandra.db.marshal.LongType" => Ok(ColumnType::BigInt),
            "org.apache.cassandra.db.marshal.SimpleDateType" => Ok(ColumnType::Date),
            "org.apache.cassandra.db.marshal.ShortType" => Ok(ColumnType::SmallInt),
            "org.apache.cassandra.db.marshal.UTF8Type" => Ok(ColumnType::Text),
            "org.apache.cassandra.db.marshal.ByteType" => Ok(ColumnType::TinyInt),
            "org.apache.cassandra.db.marshal.UUIDType" => Ok(ColumnType::Uuid),
            "org.apache.cassandra.db.marshal.TimeUUIDType" => Ok(ColumnType::Timeuuid),
            "org.apache.cassandra.db.marshal.SmallIntType" => Ok(ColumnType::SmallInt),
            "org.apache.cassandra.db.marshal.TinyIntType" => Ok(ColumnType::TinyInt),
            "org.apache.cassandra.db.marshal.TimeType" => Ok(ColumnType::Time),
            "org.apache.cassandra.db.marshal.TimestampType" => Ok(ColumnType::Timestamp),
            _ => Err(CqlTypeParseError::AbstractTypeParseError()),
        }
    }

    fn get_type_parameters(&mut self) -> Result<Vec<ColumnType<'result>>, CqlTypeParseError> {
        let mut parameters = Vec::new();
        if self.is_eos() {
            return Ok(parameters);
        }
        if self.str.as_bytes()[self.pos] != b'(' {
            return Err(CqlTypeParseError::AbstractTypeParseError());
        }
        self.pos += 1;
        loop {
            if !self.skip_blank_and_comma() {
                return Err(CqlTypeParseError::AbstractTypeParseError());
            }
            if self.str.as_bytes()[self.pos] == b')' {
                self.pos += 1;
                return Ok(parameters);
            }
            let typ = self.do_parse()?;
            parameters.push(typ);
        }
    }

    fn get_vector_parameters(&mut self) -> Result<(ColumnType<'result>, u32), CqlTypeParseError> {
        if self.is_eos() || self.str.as_bytes()[self.pos] != b'(' {
            return Err(CqlTypeParseError::AbstractTypeParseError());
        }
        self.pos += 1;
        self.skip_blank_and_comma();
        if self.str.as_bytes()[self.pos] == b')' {
            return Err(CqlTypeParseError::AbstractTypeParseError());
        }

        let typ = self.do_parse()?;
        self.skip_blank_and_comma();
        let start = self.pos;
        while !self.is_eos() && char::is_numeric(self.char_at_pos()) {
            self.pos += 1;
        }
        let len = self.str[start..self.pos]
            .parse::<u32>()
            .map_err(|_| CqlTypeParseError::AbstractTypeParseError())?;
        if self.is_eos() || self.str.as_bytes()[self.pos] != b')' {
            return Err(CqlTypeParseError::AbstractTypeParseError());
        }
        self.pos += 1;
        Ok((typ, len))
    }

    fn from_hex(s: Cow<'result, str>) -> Result<Vec<u8>, CqlTypeParseError> {
        if s.len() % 2 != 0 {
            return Err(CqlTypeParseError::AbstractTypeParseError());
        }
        for c in s.chars() {
            if !c.is_ascii_hexdigit() {
                return Err(CqlTypeParseError::AbstractTypeParseError());
            }
        }
        let mut bytes = Vec::new();
        for i in 0..s.len() / 2 {
            let byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
                .map_err(|_| CqlTypeParseError::AbstractTypeParseError())?;
            bytes.push(byte);
        }
        Ok(bytes)
    }

    fn get_udt_parameters(&mut self) -> Result<UDTParameters<'result>, CqlTypeParseError> {
        if self.is_eos() || self.str.as_bytes()[self.pos] != b'(' {
            return Err(CqlTypeParseError::AbstractTypeParseError());
        }
        self.pos += 1;

        self.skip_blank_and_comma();
        let keyspace = self.read_next_identifier();
        self.skip_blank_and_comma();
        let hex_name = &TypeParser::from_hex(self.read_next_identifier())?;
        let name = from_utf8(hex_name).map_err(|_| CqlTypeParseError::AbstractTypeParseError())?;
        let mut fields = Vec::new();
        loop {
            if !self.skip_blank_and_comma() {
                return Err(CqlTypeParseError::AbstractTypeParseError());
            }
            if self.str.as_bytes()[self.pos] == b')' {
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
    ) -> Result<ColumnType<'result>, CqlTypeParseError> {
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
                    return Err(CqlTypeParseError::AbstractTypeParseError());
                }
                Ok(ColumnType::List(Box::new(params.remove(0))))
            }
            "org.apache.cassandra.db.marshal.SetType" => {
                let mut params = self.get_type_parameters()?;
                if params.len() != 1 {
                    return Err(CqlTypeParseError::AbstractTypeParseError());
                }
                Ok(ColumnType::Set(Box::new(params.remove(0))))
            }
            "org.apache.cassandra.db.marshal.MapType" => {
                let mut params = self.get_type_parameters()?;
                if params.len() != 2 {
                    return Err(CqlTypeParseError::AbstractTypeParseError());
                }
                Ok(ColumnType::Map(
                    Box::new(params.remove(0)),
                    Box::new(params.remove(0)),
                ))
            }
            "org.apache.cassandra.db.marshal.TupleType" => {
                let params = self.get_type_parameters()?;
                if params.is_empty() {
                    return Err(CqlTypeParseError::AbstractTypeParseError());
                }
                Ok(ColumnType::Tuple(params))
            }
            "org.apache.cassandra.db.marshal.VectorType" => {
                let (typ, len) = self.get_vector_parameters()?;
                Ok(ColumnType::Vector(Box::new(typ), len))
            }
            "org.apache.cassandra.db.marshal.UserType" => {
                let (keyspace, name, fields) = self.get_udt_parameters()?;
                Ok(ColumnType::UserDefinedType {
                    type_name: name,
                    keyspace,
                    field_types: fields,
                })
            }
            _ => Err(CqlTypeParseError::AbstractTypeParseError()),
        }
    }

    fn do_parse(&mut self) -> Result<ColumnType<'result>, CqlTypeParseError> {
        self.skip_blank();

        let mut name = self.read_next_identifier();
        if name.is_empty() {
            if !self.is_eos() {
                return Err(CqlTypeParseError::AbstractTypeParseError());
            }
            return Ok(ColumnType::Blob);
        }

        if !self.is_eos() && self.str.as_bytes()[self.pos] == b':' {
            self.pos += 1;
            let _ = usize::from_str_radix(&name, 16)
                .map_err(|_| CqlTypeParseError::AbstractTypeParseError());
            name = self.read_next_identifier();
        }
        self.skip_blank();
        if !self.is_eos() && self.str.as_bytes()[self.pos] == b'(' {
            self.get_complex_abstract_type(name)
        } else {
            TypeParser::get_simple_abstract_type(name)
        }
    }
}
