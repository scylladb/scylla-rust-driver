#![no_implicit_prelude]

// Macro that is given a crate name and tests it for hygiene
macro_rules! test_crate {
    ($name:ident) => {
        extern crate $name as _scylla;

        #[derive(
            _scylla::DeserializeRow,
            _scylla::DeserializeValue,
            _scylla::SerializeValue,
            _scylla::SerializeRow,
            PartialEq,
            Debug,
        )]
        #[scylla(crate = _scylla)]
        struct TestStruct {
            a: ::core::primitive::i32,
        }
        #[test]
        fn test_impl_traits() {
            use _scylla::deserialize::row::DeserializeRow;
            use _scylla::deserialize::value::DeserializeValue;
            use _scylla::serialize::row::SerializeRow;
            use _scylla::serialize::value::SerializeValue;

            fn derived<T>()
            where
                T: for<'x> DeserializeRow<'x, 'x>
                    + for<'x> DeserializeValue<'x, 'x>
                    + SerializeValue
                    + SerializeRow,
            {
            }
            derived::<TestStruct>();
        }
        #[test]
        fn test_row_ser_deser() {
            use crate::utils::setup_tracing;
            use ::bytes::Bytes;
            use ::core::primitive::u8;
            use ::std::assert_eq;
            use ::std::vec::Vec;
            use _scylla::deserialize::row::ColumnIterator;
            use _scylla::deserialize::row::DeserializeRow;
            use _scylla::deserialize::FrameSlice;
            use _scylla::frame::response::result::ColumnSpec;
            use _scylla::frame::response::result::ColumnType;
            use _scylla::frame::response::result::NativeType;
            use _scylla::frame::response::result::TableSpec;
            use _scylla::serialize::row::RowSerializationContext;
            use _scylla::serialize::row::SerializeRow;
            use _scylla::serialize::writers::RowWriter;

            setup_tracing();

            let test_struct = TestStruct { a: 16 };
            let tuple_with_same_layout: (i32,) = (16,);
            let column_types = &[ColumnSpec::borrowed(
                "a",
                ColumnType::Native(NativeType::Int),
                TableSpec::borrowed("ks", "table"),
            )];
            let ctx = RowSerializationContext::from_specs(column_types);

            let mut buf_struct = Vec::<u8>::new();
            {
                let mut writer = RowWriter::new(&mut buf_struct);
                SerializeRow::serialize(&test_struct, &ctx, &mut writer).unwrap();
            }

            let mut buf_tuple = Vec::<u8>::new();
            {
                let mut writer = RowWriter::new(&mut buf_tuple);
                SerializeRow::serialize(&tuple_with_same_layout, &ctx, &mut writer).unwrap();
            }

            assert_eq!(buf_struct, buf_tuple);

            {
                assert!(<TestStruct as DeserializeRow>::type_check(column_types).is_ok());
                let deserialized_struct: TestStruct =
                    <TestStruct as DeserializeRow>::deserialize(ColumnIterator::new(
                        column_types,
                        FrameSlice::new(&Bytes::copy_from_slice(&buf_struct)),
                    ))
                    .unwrap();
                assert_eq!(test_struct, deserialized_struct);
            }

            {
                assert!(<(i32,) as DeserializeRow>::type_check(column_types).is_ok());
                let deserialized_tuple: (i32,) =
                    <(i32,) as DeserializeRow>::deserialize(ColumnIterator::new(
                        column_types,
                        FrameSlice::new(&Bytes::copy_from_slice(&buf_tuple)),
                    ))
                    .unwrap();
                assert_eq!(tuple_with_same_layout, deserialized_tuple);
            }
        }

        #[test]
        fn test_value_ser_deser() {
            use crate::utils::setup_tracing;
            use ::bytes::Bytes;
            use ::core::primitive::u8;
            use ::std::assert_eq;
            use ::std::convert::Into;
            use ::std::option::Option::Some;
            use ::std::string::ToString;
            use ::std::sync::Arc;
            use ::std::vec;
            use ::std::vec::Vec;
            use ::tracing::info;
            use _scylla::deserialize::value::DeserializeValue;
            use _scylla::deserialize::FrameSlice;
            use _scylla::frame::response::result::{ColumnType, NativeType, UserDefinedType};
            use _scylla::serialize::value::SerializeValue;
            use _scylla::serialize::writers::CellWriter;
            use _scylla::value::CqlValue;

            setup_tracing();

            let test_struct = TestStruct { a: 16 };
            let value_with_same_layout: CqlValue = CqlValue::UserDefinedType {
                keyspace: "some_ks".to_string(),
                name: "some_type".to_string(),
                fields: vec![("a".to_string(), Some(CqlValue::Int(16)))],
            };
            let udt_type = ColumnType::UserDefinedType {
                frozen: false,
                definition: Arc::new(UserDefinedType {
                    name: "some_type".into(),
                    keyspace: "some_ks".into(),
                    field_types: vec![("a".into(), ColumnType::Native(NativeType::Int))],
                })
            };

            let mut buf_struct = Vec::<u8>::new();
            {
                let writer = CellWriter::new(&mut buf_struct);
                SerializeValue::serialize(&test_struct, &udt_type, writer).unwrap();
                info!("struct buffer: {:?}", buf_struct);
            }

            let mut buf_value = Vec::<u8>::new();
            {
                let writer = CellWriter::new(&mut buf_value);
                SerializeValue::serialize(&value_with_same_layout, &udt_type, writer).unwrap();
                info!("value buffer: {:?}", buf_struct);
            }

            assert_eq!(buf_struct, buf_value);

            {
                assert!(<TestStruct as DeserializeValue>::type_check(&udt_type).is_ok());
                let bytes_buf = Bytes::copy_from_slice(&buf_struct);
                let slice = FrameSlice::new(&bytes_buf).read_cql_bytes().unwrap();
                let deserialized_struct: TestStruct =
                    <TestStruct as DeserializeValue>::deserialize(&udt_type, slice).unwrap();
                assert_eq!(test_struct, deserialized_struct);
            }

            {
                assert!(<CqlValue as DeserializeValue>::type_check(&udt_type).is_ok());
                let bytes_buf = Bytes::copy_from_slice(&buf_value);
                let slice = FrameSlice::new(&bytes_buf).read_cql_bytes().unwrap();
                let deserialized_value: CqlValue =
                    <CqlValue as DeserializeValue>::deserialize(&udt_type, slice).unwrap();
                assert_eq!(value_with_same_layout, deserialized_value);
            }
        }

        // The purpose of this test is to verify that the important types are
        // correctly re-exported in `scylla` crate.
        #[test]
        fn test_types_imports() {
            #[allow(unused_imports)]
            use _scylla::frame::response::result::{CollectionType, ColumnType, NativeType, UserDefinedType};
            #[allow(unused_imports)]
            use _scylla::value::{
                Counter, CqlDate, CqlDecimal, CqlDecimalBorrowed, CqlDuration, CqlTime, CqlTimestamp,
                CqlTimeuuid, CqlValue, CqlVarint, CqlVarintBorrowed, MaybeUnset, Row, Unset, ValueOverflow
            };
        }

        // Test attributes for value struct with name flavor
        #[derive(
            _scylla::DeserializeValue, _scylla::SerializeValue, PartialEq, Debug,
        )]
        #[scylla(crate = _scylla)]
        #[allow(dead_code)] // TODO: Change to expect after bumping MSRV to 1.89
        struct TestStructByName {
            a: ::core::primitive::i32,
            #[scylla(allow_missing)]
            b: ::core::primitive::i32,
            #[scylla(default_when_null)]
            c: ::core::primitive::i32,
            #[scylla(skip)]
            d: ::core::primitive::i32,
            #[scylla(rename = "f")]
            e: ::core::primitive::i32,
            g: ::core::primitive::i32,
        }

        // Test attributes for value struct with strict name flavor
        #[derive(
            _scylla::DeserializeValue, _scylla::SerializeValue, PartialEq, Debug,
        )]
        #[scylla(crate = _scylla, forbid_excess_udt_fields)]
        #[allow(dead_code)] // TODO: Change to expect after bumping MSRV to 1.89
        struct TestStructByNameStrict {
            a: ::core::primitive::i32,
            #[scylla(allow_missing)]
            b: ::core::primitive::i32,
            #[scylla(default_when_null)]
            c: ::core::primitive::i32,
            #[scylla(skip)]
            d: ::core::primitive::i32,
            #[scylla(rename = "f")]
            e: ::core::primitive::i32,
            g: ::core::primitive::i32,
        }

        // Test attributes for value struct with ordered flavor
        #[derive(
            _scylla::DeserializeValue, _scylla::SerializeValue, PartialEq, Debug,
        )]
        #[scylla(crate = _scylla, flavor = "enforce_order")]
        #[allow(dead_code)] // TODO: Change to expect after bumping MSRV to 1.89
        struct TestStructOrdered {
            a: ::core::primitive::i32,
            #[scylla(allow_missing)]
            b: ::core::primitive::i32,
            #[scylla(default_when_null)]
            c: ::core::primitive::i32,
            #[scylla(skip)]
            d: ::core::primitive::i32,
            #[scylla(rename = "f")]
            e: ::core::primitive::i32,
            g: ::core::primitive::i32,
        }

         // Test attributes for value struct with ordered flavor
        #[derive(
            _scylla::DeserializeValue, _scylla::SerializeValue, PartialEq, Debug,
        )]
        #[scylla(crate = _scylla, flavor = "enforce_order")]
        #[scylla(allow_missing)]
        struct TestStructOrderedAllowedMissing {
            a: ::core::primitive::i32,
            b: ::core::primitive::i32,
            #[scylla(default_when_null)]
            c: ::core::primitive::i32,
            #[scylla(skip)]
            d: ::core::primitive::i32,
            #[scylla(rename = "f")]
            e: ::core::primitive::i32,
            g: ::core::primitive::i32,
        }

        // Test attributes for value struct with strict ordered flavor
        #[derive(
            _scylla::DeserializeValue, _scylla::SerializeValue, PartialEq, Debug,
        )]
        #[scylla(crate = _scylla, flavor = "enforce_order", forbid_excess_udt_fields)]
        #[allow(dead_code)] // TODO: Change to expect after bumping MSRV to 1.89
        struct TestStructOrderedStrict {
            a: ::core::primitive::i32,
            #[scylla(allow_missing)]
            b: ::core::primitive::i32,
            #[scylla(default_when_null)]
            c: ::core::primitive::i32,
            #[scylla(skip)]
            d: ::core::primitive::i32,
            #[scylla(rename = "f")]
            e: ::core::primitive::i32,
            g: ::core::primitive::i32,
        }

        // Test attributes for value struct with ordered flavor and skipped name checks
        #[derive(
            _scylla::DeserializeValue, _scylla::SerializeValue, PartialEq, Debug,
        )]
        #[scylla(crate = _scylla, flavor = "enforce_order", skip_name_checks)]
        #[allow(dead_code)] // TODO: Change to expect after bumping MSRV to 1.89
        struct TestStructOrderedSkipped {
            a: ::core::primitive::i32,
            b: ::core::primitive::i32,
            #[scylla(default_when_null)]
            c: ::core::primitive::i32,
            d: ::core::primitive::i32,
            #[scylla(skip)]
            e: ::core::primitive::i32,
            #[scylla(allow_missing)]
            g: ::core::primitive::i32,
        }

        // Test attributes for value struct with strict ordered flavor and skipped name checks
        #[derive(
            _scylla::DeserializeValue, _scylla::SerializeValue, PartialEq, Debug,
        )]
        #[scylla(crate = _scylla, flavor = "enforce_order", skip_name_checks, forbid_excess_udt_fields)]
        #[allow(dead_code)] // TODO: Change to expect after bumping MSRV to 1.89
        struct TestStructOrderedStrictSkipped {
            a: ::core::primitive::i32,
            b: ::core::primitive::i32,
            #[scylla(default_when_null)]
            c: ::core::primitive::i32,
            d: ::core::primitive::i32,
            #[scylla(skip)]
            e: ::core::primitive::i32,
            #[scylla(allow_missing)]
            g: ::core::primitive::i32,
        }

        // Test attributes for row struct with name flavor
        #[derive(
            _scylla::DeserializeRow, _scylla::SerializeRow, PartialEq, Debug,
        )]
        #[scylla(crate = _scylla)]
        // We would like to have this `expect(dead_code)`, but it does not work for
        // SerializeRow with by-name flavor. See https://github.com/scylladb/scylla-rust-driver/issues/1429
        // After fixing the above issue, we can add allow(dead_code)
        // After also bumping MSRV to 1.89+ we can add expect(dead_code).
        struct TestRowByName {
            #[scylla(skip)]
            a: ::core::primitive::i32,
            #[scylla(rename = "f")]
            b: ::core::primitive::i32,
            c: ::core::primitive::i32,
            #[scylla(default_when_null)]
            d: ::core::primitive::i32,
            #[scylla(allow_missing)]
            e: ::core::primitive::i32,
        }
        // Test attributes for row struct with name flavor
        #[derive(
            _scylla::DeserializeRow, _scylla::SerializeRow, PartialEq, Debug,
        )]
        #[scylla(crate = _scylla)]
        #[scylla(allow_missing)]
        struct TestRowByNameWithMissing {
            #[scylla(skip)]
            a: ::core::primitive::i32,
            #[scylla(rename = "f")]
            b: ::core::primitive::i32,
            c: ::core::primitive::i32,
            #[scylla(default_when_null)]
            d: ::core::primitive::i32,
            e: ::core::primitive::i32,
        }

        // Test attributes for row struct with ordered flavor
        #[derive(
            _scylla::DeserializeRow, _scylla::SerializeRow, PartialEq, Debug,
        )]
        #[scylla(crate = _scylla, flavor = "enforce_order")]
        #[allow(dead_code)] // TODO: Change to expect after bumping MSRV to 1.89
        struct TestRowByOrder {
            #[scylla(skip)]
            a: ::core::primitive::i32,
            #[scylla(rename = "f")]
            b: ::core::primitive::i32,
            c: ::core::primitive::i32,
            #[scylla(default_when_null)]
            d: ::core::primitive::i32,
        }

        // Test attributes for row struct with ordered flavor and skipped name checks
        #[derive(
            _scylla::DeserializeRow, _scylla::SerializeRow, PartialEq, Debug,
        )]
        #[scylla(crate = _scylla, flavor = "enforce_order", skip_name_checks)]
        #[allow(dead_code)] // TODO: Change to expect after bumping MSRV to 1.89
        struct TestRowByOrderSkipped {
            #[scylla(skip)]
            a: ::core::primitive::i32,
            b: ::core::primitive::i32,
            c: ::core::primitive::i32,
            #[scylla(default_when_null)]
            d: ::core::primitive::i32,
        }
    };
}

mod scylla_hygiene {
    test_crate!(scylla);
}
mod scylla_cql_hygiene {
    test_crate!(scylla_cql);
}
