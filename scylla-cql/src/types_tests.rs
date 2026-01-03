mod derive_macros_integration {
    mod value {
        use bytes::Bytes;

        use crate::deserialize::value::tests::{deserialize, udt_def_with_fields};
        use crate::frame::response::result::{ColumnType, NativeType};
        use crate::serialize::value::tests::do_serialize;

        #[test]
        fn derive_serialize_and_deserialize_value_loose_ordering() {
            #[derive(
                scylla_macros::DeserializeValue, scylla_macros::SerializeValue, PartialEq, Eq, Debug,
            )]
            #[scylla(crate = "crate")]
            struct Udt<'a> {
                a: &'a str,
                #[scylla(skip)]
                x: String,
                #[scylla(allow_missing)]
                b: Option<i32>,
                #[scylla(default_when_null)]
                c: i64,
            }

            let original_udt = Udt {
                a: "The quick brown fox",
                x: String::from("THIS SHOULD NOT BE (DE)SERIALIZED"),
                b: Some(42),
                c: 2137,
            };

            let tests = [
                // All fields present
                (
                    udt_def_with_fields([
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("b", ColumnType::Native(NativeType::Int)),
                        ("c", ColumnType::Native(NativeType::BigInt)),
                    ]),
                    Udt {
                        x: String::new(),
                        ..original_udt
                    },
                ),
                //
                // One field missing:
                // - ignored during serialization,
                // - default-initialized during deserialization.
                (
                    udt_def_with_fields([
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("c", ColumnType::Native(NativeType::BigInt)),
                    ]),
                    Udt {
                        x: String::new(),
                        b: None,
                        ..original_udt
                    },
                ),
                //
                // UDT fields switched - should still work.
                (
                    udt_def_with_fields([
                        ("b", ColumnType::Native(NativeType::Int)),
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("c", ColumnType::Native(NativeType::BigInt)),
                    ]),
                    Udt {
                        x: String::new(),
                        ..original_udt
                    },
                ),
            ];
            for (typ, expected_udt) in tests {
                let serialized_udt = Bytes::from(do_serialize(&original_udt, &typ));
                let deserialized_udt = deserialize::<Udt<'_>>(&typ, &serialized_udt).unwrap();

                assert_eq!(deserialized_udt, expected_udt);
            }
        }

        #[test]
        fn derive_serialize_and_deserialize_value_strict_ordering() {
            #[derive(
                scylla_macros::DeserializeValue, scylla_macros::SerializeValue, PartialEq, Eq, Debug,
            )]
            #[scylla(crate = "crate", flavor = "enforce_order")]
            struct Udt<'a> {
                #[scylla(allow_missing)]
                #[scylla(default_when_null)]
                a: &'a str,
                #[scylla(skip)]
                x: String,
                #[scylla(allow_missing)]
                b: Option<i32>,
            }

            let original_udt = Udt {
                a: "The quick brown fox",
                x: String::from("THIS SHOULD NOT BE (DE)SERIALIZED"),
                b: Some(42),
            };

            let tests = [
                // All fields present
                (
                    udt_def_with_fields([
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("b", ColumnType::Native(NativeType::Int)),
                    ]),
                    Udt {
                        x: String::new(),
                        ..original_udt
                    },
                ),
                //
                // An excess field at the end of UDT
                (
                    udt_def_with_fields([
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("b", ColumnType::Native(NativeType::Int)),
                        ("d", ColumnType::Native(NativeType::Boolean)),
                    ]),
                    Udt {
                        x: String::new(),
                        ..original_udt
                    },
                ),
                //
                // Missing non-required fields
                (
                    udt_def_with_fields([("a", ColumnType::Native(NativeType::Text))]),
                    Udt {
                        x: String::new(),
                        b: None,
                        ..original_udt
                    },
                ),
                //
                // An excess field at the end of UDT instead of non-required fields
                (
                    udt_def_with_fields([("d", ColumnType::Native(NativeType::Boolean))]),
                    Udt {
                        x: String::new(),
                        a: "",
                        b: None,
                    },
                ),
            ];
            for (typ, expected_udt) in tests {
                let serialized_udt = Bytes::from(do_serialize(&original_udt, &typ));
                let deserialized_udt = deserialize::<Udt<'_>>(&typ, &serialized_udt).unwrap();

                assert_eq!(deserialized_udt, expected_udt);
            }
        }

        #[test]
        fn derive_ser_de_with_metadata_present_but_value_missing_during_deser() {
            #[derive(scylla_macros::SerializeValue, Debug)]
            #[scylla(crate = "crate")]
            struct SerUdt<'a> {
                a: &'a str,
            }

            #[derive(scylla_macros::DeserializeValue, Debug, PartialEq, Eq)]
            #[scylla(crate = "crate")]
            struct DeserUdt<'a> {
                a: &'a str,
                b: Option<i32>,
            }

            let ser_udt = SerUdt {
                a: "The quick brown fox",
            };
            // Do not serialize `b` field.
            let serialization_type =
                udt_def_with_fields([("a", ColumnType::Native(NativeType::Text))]);

            let expected_deserialized_udt = DeserUdt {
                a: "The quick brown fox",
                b: None,
            };
            // Deserialize `b` field. The metadata is present, but value is missing (i.e. `UdtIterator::next()` returns Ok(None)).
            // This is possible, as specified by https://github.com/apache/cassandra/blob/4a80daf32eb4226d9870b914779a1fc007479da6/doc/native_protocol_v4.spec#L1003.
            // It should fallback to null (b: None).
            let deserialization_type = udt_def_with_fields([
                ("a", ColumnType::Native(NativeType::Text)),
                ("b", ColumnType::Native(NativeType::Int)),
            ]);

            let serialized_udt = Bytes::from(do_serialize(&ser_udt, &serialization_type));

            let deserialized_udt =
                deserialize::<DeserUdt<'_>>(&deserialization_type, &serialized_udt).unwrap();
            assert_eq!(deserialized_udt, expected_deserialized_udt);
        }
    }

    mod row {
        use bytes::Bytes;

        use crate::deserialize::row::tests::deserialize;
        use crate::deserialize::tests::spec;
        use crate::frame::response::result::{ColumnType, NativeType};
        use crate::serialize::row::tests::do_serialize;

        #[test]
        fn derive_serialize_and_deserialize_row_loose_ordering() {
            #[derive(
                scylla_macros::DeserializeRow, scylla_macros::SerializeRow, PartialEq, Eq, Debug,
            )]
            #[scylla(crate = "crate")]
            struct MyRow<'a> {
                a: &'a str,
                #[scylla(skip)]
                x: String,
                b: Option<i32>,
                c: i64,
            }

            let original_row = MyRow {
                a: "The quick brown fox",
                x: String::from("THIS SHOULD NOT BE (DE)SERIALIZED"),
                b: Some(42),
                c: 2137,
            };

            let tests = [
                // All columns present
                (
                    &[
                        spec("a", ColumnType::Native(NativeType::Text)),
                        spec("b", ColumnType::Native(NativeType::Int)),
                        spec("c", ColumnType::Native(NativeType::BigInt)),
                    ][..],
                    MyRow {
                        x: String::new(),
                        ..original_row
                    },
                ),
                //
                // Columns switched - should still work.
                (
                    &[
                        spec("b", ColumnType::Native(NativeType::Int)),
                        spec("a", ColumnType::Native(NativeType::Text)),
                        spec("c", ColumnType::Native(NativeType::BigInt)),
                    ],
                    MyRow {
                        x: String::new(),
                        ..original_row
                    },
                ),
            ];
            for (typ, expected_row) in tests {
                let serialized_row = Bytes::from(do_serialize(&original_row, typ));
                let deserialized_row = deserialize::<MyRow<'_>>(typ, &serialized_row).unwrap();

                assert_eq!(deserialized_row, expected_row);
            }
        }

        #[test]
        fn derive_serialize_and_deserialize_row_strict_ordering() {
            #[derive(
                scylla_macros::DeserializeRow, scylla_macros::SerializeRow, PartialEq, Eq, Debug,
            )]
            #[scylla(crate = "crate", flavor = "enforce_order")]
            struct MyRow<'a> {
                a: &'a str,
                #[scylla(skip)]
                x: String,
                b: Option<i32>,
            }

            let original_row = MyRow {
                a: "The quick brown fox",
                x: String::from("THIS SHOULD NOT BE (DE)SERIALIZED"),
                b: Some(42),
            };

            let tests = [
                // All columns present
                (
                    &[
                        spec("a", ColumnType::Native(NativeType::Text)),
                        spec("b", ColumnType::Native(NativeType::Int)),
                    ][..],
                    MyRow {
                        x: String::new(),
                        ..original_row
                    },
                ),
            ];
            for (typ, expected_row) in tests {
                let serialized_row = Bytes::from(do_serialize(&original_row, typ));
                let deserialized_row = deserialize::<MyRow<'_>>(typ, &serialized_row).unwrap();

                assert_eq!(deserialized_row, expected_row);
            }
        }
    }
}

mod from_row_tuple_tests {
    use crate::deserialize::FrameSlice;
    use crate::deserialize::row::{ColumnIterator, DeserializeRow};
    use crate::deserialize::tests::spec;
    use crate::frame::response::result::{ColumnType, NativeType};
    use bytes::Bytes;
    use scylla_macros::DeserializeRow;

    fn make_row_bytes(values: &[Option<&[u8]>]) -> Bytes {
        let mut buf = Vec::new();
        for v in values {
            match v {
                Some(bytes) => {
                    buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    buf.extend_from_slice(bytes);
                }
                None => {
                    buf.extend_from_slice(&(-1i32).to_be_bytes());
                }
            }
        }
        Bytes::from(buf)
    }

    #[derive(DeserializeRow, Debug, PartialEq)]
    #[scylla(crate = "crate")]
    struct Wrapper(i32);

    #[derive(DeserializeRow, Debug, PartialEq)]
    #[scylla(crate = "crate")]
    struct MultiTuple(i32, String, f64);

    #[derive(DeserializeRow, Debug, PartialEq)]
    #[scylla(crate = "crate")]
    struct NullableTuple(Option<i32>);

    #[derive(DeserializeRow, Debug, PartialEq)]
    #[scylla(crate = "crate")]
    struct GenericTuple<T>(T);

    #[test]
    fn test_single_wrapper() {
        let specs = vec![spec("col1", ColumnType::Native(NativeType::Int))];
        let val = 42i32.to_be_bytes();
        let data = make_row_bytes(&[Some(&val)]);
        let slice = FrameSlice::new(&data);
        let iter = ColumnIterator::new(&specs, slice);
        let result = Wrapper::deserialize(iter).unwrap();
        assert_eq!(result, Wrapper(42));
    }

    #[test]
    fn test_multi_tuple() {
        let specs = vec![
            spec("a", ColumnType::Native(NativeType::Int)),
            spec("b", ColumnType::Native(NativeType::Text)),
            spec("c", ColumnType::Native(NativeType::Double)),
        ];
        let v1 = 10i32.to_be_bytes();
        let v2 = b"test";
        let v3 = 3.5f64.to_be_bytes();
        let data = make_row_bytes(&[Some(&v1), Some(v2), Some(&v3)]);
        let slice = FrameSlice::new(&data);
        let iter = ColumnIterator::new(&specs, slice);
        let result = MultiTuple::deserialize(iter).unwrap();
        assert_eq!(result, MultiTuple(10, "test".to_string(), 3.5));
    }

    #[test]
    fn test_nullable_tuple() {
        let specs = vec![spec("col1", ColumnType::Native(NativeType::Int))];
        let val = 123i32.to_be_bytes();
        let data_some = make_row_bytes(&[Some(&val)]);
        let iter_some = ColumnIterator::new(&specs, FrameSlice::new(&data_some));
        assert_eq!(
            NullableTuple::deserialize(iter_some).unwrap(),
            NullableTuple(Some(123))
        );
        let data_none = make_row_bytes(&[None]);
        let iter_none = ColumnIterator::new(&specs, FrameSlice::new(&data_none));
        assert_eq!(
            NullableTuple::deserialize(iter_none).unwrap(),
            NullableTuple(None)
        );
    }

    #[test]
    fn test_generic_tuple() {
        let specs = vec![spec("col1", ColumnType::Native(NativeType::Int))];
        let val = 999i32.to_be_bytes();
        let data = make_row_bytes(&[Some(&val)]);
        let iter = ColumnIterator::new(&specs, FrameSlice::new(&data));
        let result = GenericTuple::<i32>::deserialize(iter).unwrap();
        assert_eq!(result, GenericTuple(999));
    }

    #[test]
    fn test_type_check_mismatch() {
        let specs = vec![spec("col1", ColumnType::Native(NativeType::Text))];
        let result = Wrapper::type_check(&specs);
        assert!(
            result.is_err(),
            "Should return type check error on type mismatch"
        );
    }

    #[test]
    fn test_column_count_mismatch_too_few() {
        let specs = vec![];
        let result = Wrapper::type_check(&specs);
        assert!(result.is_err(), "Should return error when too few columns");
    }

    #[test]
    fn test_column_count_mismatch_too_many() {
        let specs = vec![
            spec("col1", ColumnType::Native(NativeType::Int)),
            spec("col2", ColumnType::Native(NativeType::Int)),
        ];
        let result = Wrapper::type_check(&specs);
        assert!(result.is_err(), "Should return error when too many columns");
    }

    #[test]
    fn test_deserialize_failure() {
        let specs = vec![spec("col1", ColumnType::Native(NativeType::Int))];
        let bad_val = vec![1u8, 2, 3];
        let data = make_row_bytes(&[Some(&bad_val)]);
        let iter = ColumnIterator::new(&specs, FrameSlice::new(&data));
        let result = Wrapper::deserialize(iter);

        assert!(
            result.is_err(),
            "Should fail deserialization on malformed data"
        );
    }
}
