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
        use crate::frame::response::result::ColumnSpec;
        use crate::frame::response::result::{ColumnType, NativeType};
        use crate::serialize::row::{RowSerializationContext, SerializeRow};
        use crate::serialize::writers::RowWriter;

        pub(crate) fn do_serialize<T: SerializeRow>(t: T, columns: &[ColumnSpec]) -> Vec<u8> {
            let ctx = RowSerializationContext::from_specs(columns);
            let mut ret = Vec::new();
            let mut builder = RowWriter::new(&mut ret);
            t.serialize(&ctx, &mut builder).unwrap();
            ret
        }

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

        mod serialize {
            use assert_matches::assert_matches;

            use crate::SerializeRow;
            use crate::deserialize::tests::spec;
            use crate::frame::response::result::{CollectionType, ColumnType, NativeType};
            use crate::serialize::row::{
                BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
                BuiltinTypeCheckErrorKind, RowSerializationContext, SerializeRow,
            };
            use crate::serialize::value::SerializeValue;
            use crate::serialize::writers::RowWriter;

            use super::do_serialize;

            // Do not remove. It's not used in tests but we keep it here to check that
            // we properly ignore warnings about unused variables, unnecessary `mut`s
            // etc. that usually pop up when generating code for empty structs.
            #[derive(SerializeRow)]
            #[scylla(crate = crate)]
            #[allow(dead_code)] // TODO: Change to expect after bumping MSRV to 1.90
            struct TestRowWithNoColumns {}

            #[derive(SerializeRow, Debug, PartialEq, Eq, Default)]
            #[scylla(crate = crate)]
            struct TestRowWithColumnSorting {
                a: String,
                b: i32,
                c: Vec<i64>,
            }

            #[test]
            fn test_row_serialization_with_column_sorting_correct_order() {
                let spec = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec(
                        "c",
                        ColumnType::Collection {
                            frozen: false,
                            typ: CollectionType::List(Box::new(ColumnType::Native(
                                NativeType::BigInt,
                            ))),
                        },
                    ),
                ];

                let reference = do_serialize(("Ala ma kota", 42i32, vec![1i64, 2i64, 3i64]), &spec);
                let row = do_serialize(
                    TestRowWithColumnSorting {
                        a: "Ala ma kota".to_owned(),
                        b: 42,
                        c: vec![1, 2, 3],
                    },
                    &spec,
                );

                assert_eq!(reference, row);
            }

            #[test]
            fn test_row_serialization_with_column_sorting_incorrect_order() {
                // The order of two last columns is swapped
                let spec = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec(
                        "c",
                        ColumnType::Collection {
                            frozen: false,
                            typ: CollectionType::List(Box::new(ColumnType::Native(
                                NativeType::BigInt,
                            ))),
                        },
                    ),
                    spec("b", ColumnType::Native(NativeType::Int)),
                ];

                let reference = do_serialize(("Ala ma kota", vec![1i64, 2i64, 3i64], 42i32), &spec);
                let row = do_serialize(
                    TestRowWithColumnSorting {
                        a: "Ala ma kota".to_owned(),
                        b: 42,
                        c: vec![1, 2, 3],
                    },
                    &spec,
                );

                assert_eq!(reference, row);
            }

            #[test]
            fn test_row_serialization_failing_type_check() {
                let row = TestRowWithColumnSorting::default();
                let mut data = Vec::new();
                let mut row_writer = RowWriter::new(&mut data);

                let spec_without_c = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    // Missing column c
                ];

                let ctx = RowSerializationContext::from_specs(&spec_without_c);
                let err = <_ as SerializeRow>::serialize(&row, &ctx, &mut row_writer).unwrap_err();
                let err = err.downcast_ref::<BuiltinTypeCheckError>().unwrap();
                assert_matches!(err.kind, BuiltinTypeCheckErrorKind::NoColumnWithName { .. });

                let spec_duplicate_column = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec(
                        "c",
                        ColumnType::Collection {
                            frozen: false,
                            typ: CollectionType::List(Box::new(ColumnType::Native(
                                NativeType::BigInt,
                            ))),
                        },
                    ),
                    // Unexpected last column
                    spec("d", ColumnType::Native(NativeType::Counter)),
                ];

                let ctx = RowSerializationContext::from_specs(&spec_duplicate_column);
                let err = <_ as SerializeRow>::serialize(&row, &ctx, &mut row_writer).unwrap_err();
                let err = err.downcast_ref::<BuiltinTypeCheckError>().unwrap();
                assert_matches!(
                    err.kind,
                    BuiltinTypeCheckErrorKind::ValueMissingForColumn { .. }
                );

                let spec_wrong_type = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("c", ColumnType::Native(NativeType::TinyInt)), // Wrong type
                ];

                let ctx = RowSerializationContext::from_specs(&spec_wrong_type);
                let err = <_ as SerializeRow>::serialize(&row, &ctx, &mut row_writer).unwrap_err();
                let err = err.downcast_ref::<BuiltinSerializationError>().unwrap();
                assert_matches!(
                    err.kind,
                    BuiltinSerializationErrorKind::ColumnSerializationFailed { .. }
                );
            }

            #[derive(SerializeRow)]
            #[scylla(crate = crate)]
            struct TestRowWithGenerics<'a, T: SerializeValue> {
                a: &'a str,
                b: T,
            }

            #[test]
            fn test_row_serialization_with_generics() {
                // A minimal smoke test just to test that it works.
                fn check_with_type<T: SerializeValue + Copy>(typ: ColumnType<'static>, t: T) {
                    let spec = [
                        spec("a", ColumnType::Native(NativeType::Text)),
                        spec("b", typ),
                    ];
                    let reference = do_serialize(("Ala ma kota", t), &spec);
                    let row = do_serialize(
                        TestRowWithGenerics {
                            a: "Ala ma kota",
                            b: t,
                        },
                        &spec,
                    );
                    assert_eq!(reference, row);
                }

                check_with_type(ColumnType::Native(NativeType::Int), 123_i32);
                check_with_type(ColumnType::Native(NativeType::Double), 123_f64);
            }

            #[derive(SerializeRow, Debug, PartialEq, Eq, Default)]
            #[scylla(crate = crate, flavor = "enforce_order")]
            struct TestRowWithEnforcedOrder {
                a: String,
                b: i32,
                c: Vec<i64>,
            }

            #[test]
            fn test_row_serialization_with_enforced_order_correct_order() {
                let spec = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec(
                        "c",
                        ColumnType::Collection {
                            frozen: false,
                            typ: CollectionType::List(Box::new(ColumnType::Native(
                                NativeType::BigInt,
                            ))),
                        },
                    ),
                ];

                let reference = do_serialize(("Ala ma kota", 42i32, vec![1i64, 2i64, 3i64]), &spec);
                let row = do_serialize(
                    TestRowWithEnforcedOrder {
                        a: "Ala ma kota".to_owned(),
                        b: 42,
                        c: vec![1, 2, 3],
                    },
                    &spec,
                );

                assert_eq!(reference, row);
            }

            #[test]
            fn test_row_serialization_with_enforced_order_failing_type_check() {
                let row = TestRowWithEnforcedOrder::default();
                let mut data = Vec::new();
                let mut writer = RowWriter::new(&mut data);

                // The order of two last columns is swapped
                let specs = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec(
                        "c",
                        ColumnType::Collection {
                            frozen: false,
                            typ: CollectionType::List(Box::new(ColumnType::Native(
                                NativeType::BigInt,
                            ))),
                        },
                    ),
                    spec("b", ColumnType::Native(NativeType::Int)),
                ];
                let ctx = RowSerializationContext::from_specs(&specs);
                let err = <_ as SerializeRow>::serialize(&row, &ctx, &mut writer).unwrap_err();
                let err = err.downcast_ref::<BuiltinTypeCheckError>().unwrap();
                assert_matches!(
                    err.kind,
                    BuiltinTypeCheckErrorKind::ColumnNameMismatch { .. }
                );

                let spec_without_c = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    // Missing column c
                ];

                let ctx = RowSerializationContext::from_specs(&spec_without_c);
                let err = <_ as SerializeRow>::serialize(&row, &ctx, &mut writer).unwrap_err();
                let err = err.downcast_ref::<BuiltinTypeCheckError>().unwrap();
                assert_matches!(err.kind, BuiltinTypeCheckErrorKind::NoColumnWithName { .. });

                let spec_duplicate_column = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec(
                        "c",
                        ColumnType::Collection {
                            frozen: false,
                            typ: CollectionType::List(Box::new(ColumnType::Native(
                                NativeType::BigInt,
                            ))),
                        },
                    ),
                    // Unexpected last column
                    spec("d", ColumnType::Native(NativeType::Counter)),
                ];

                let ctx = RowSerializationContext::from_specs(&spec_duplicate_column);
                let err = <_ as SerializeRow>::serialize(&row, &ctx, &mut writer).unwrap_err();
                let err = err.downcast_ref::<BuiltinTypeCheckError>().unwrap();
                assert_matches!(
                    err.kind,
                    BuiltinTypeCheckErrorKind::ValueMissingForColumn { .. }
                );

                let spec_wrong_type = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("c", ColumnType::Native(NativeType::TinyInt)), // Wrong type
                ];

                let ctx = RowSerializationContext::from_specs(&spec_wrong_type);
                let err = <_ as SerializeRow>::serialize(&row, &ctx, &mut writer).unwrap_err();
                let err = err.downcast_ref::<BuiltinSerializationError>().unwrap();
                assert_matches!(
                    err.kind,
                    BuiltinSerializationErrorKind::ColumnSerializationFailed { .. }
                );
            }

            #[derive(SerializeRow, Debug)]
            #[scylla(crate = crate)]
            struct TestRowWithColumnRename {
                a: String,
                #[scylla(rename = "x")]
                b: i32,
            }

            #[derive(SerializeRow, Debug)]
            #[scylla(crate = crate, flavor = "enforce_order")]
            struct TestRowWithColumnRenameAndEnforceOrder {
                a: String,
                #[scylla(rename = "x")]
                b: i32,
            }

            #[test]
            fn test_row_serialization_with_column_rename() {
                let spec = [
                    spec("x", ColumnType::Native(NativeType::Int)),
                    spec("a", ColumnType::Native(NativeType::Text)),
                ];

                let reference = do_serialize((42i32, "Ala ma kota"), &spec);
                let row = do_serialize(
                    TestRowWithColumnRename {
                        a: "Ala ma kota".to_owned(),
                        b: 42,
                    },
                    &spec,
                );

                assert_eq!(reference, row);
            }

            #[test]
            fn test_row_serialization_with_column_rename_and_enforce_order() {
                let spec = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("x", ColumnType::Native(NativeType::Int)),
                ];

                let reference = do_serialize(("Ala ma kota", 42i32), &spec);
                let row = do_serialize(
                    TestRowWithColumnRenameAndEnforceOrder {
                        a: "Ala ma kota".to_owned(),
                        b: 42,
                    },
                    &spec,
                );

                assert_eq!(reference, row);
            }

            #[derive(SerializeRow, Debug)]
            #[scylla(crate = crate, flavor = "enforce_order", skip_name_checks)]
            struct TestRowWithSkippedNameChecks {
                a: String,
                b: i32,
            }

            #[test]
            fn test_row_serialization_with_skipped_name_checks() {
                let spec = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("x", ColumnType::Native(NativeType::Int)),
                ];

                let reference = do_serialize(("Ala ma kota", 42i32), &spec);
                let row = do_serialize(
                    TestRowWithSkippedNameChecks {
                        a: "Ala ma kota".to_owned(),
                        b: 42,
                    },
                    &spec,
                );

                assert_eq!(reference, row);
            }

            #[test]
            fn test_row_serialization_with_not_rust_idents() {
                #[derive(SerializeRow, Debug)]
                #[scylla(crate = crate)]
                struct RowWithTTL {
                    #[scylla(rename = "[ttl]")]
                    ttl: i32,
                }

                let spec = [spec("[ttl]", ColumnType::Native(NativeType::Int))];

                let reference = do_serialize((42i32,), &spec);
                let row = do_serialize(RowWithTTL { ttl: 42 }, &spec);

                assert_eq!(reference, row);
            }

            #[derive(SerializeRow, Debug)]
            #[scylla(crate = crate)]
            struct TestRowWithSkippedFields {
                a: String,
                b: i32,
                #[scylla(skip)]
                #[expect(dead_code)]
                skipped: Vec<String>,
                c: Vec<i64>,
            }

            #[test]
            fn test_row_serialization_with_skipped_field() {
                let spec = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec(
                        "c",
                        ColumnType::Collection {
                            frozen: false,
                            typ: CollectionType::List(Box::new(ColumnType::Native(
                                NativeType::BigInt,
                            ))),
                        },
                    ),
                ];

                let reference = do_serialize(
                    TestRowWithColumnSorting {
                        a: "Ala ma kota".to_owned(),
                        b: 42,
                        c: vec![1, 2, 3],
                    },
                    &spec,
                );
                let row = do_serialize(
                    TestRowWithSkippedFields {
                        a: "Ala ma kota".to_owned(),
                        b: 42,
                        skipped: vec!["abcd".to_owned(), "efgh".to_owned()],
                        c: vec![1, 2, 3],
                    },
                    &spec,
                );

                assert_eq!(reference, row);
            }

            #[test]
            fn test_row_serialization_nested_structs() {
                #[derive(SerializeRow, Debug)]
                #[scylla(crate = crate)]
                struct InnerColumnsOne {
                    x: i32,
                    y: f64,
                }

                #[derive(SerializeRow, Debug)]
                #[scylla(crate = crate)]
                struct InnerColumnsTwo {
                    z: bool,
                }

                #[derive(SerializeRow, Debug)]
                #[scylla(crate = crate)]
                struct OuterColumns {
                    #[scylla(flatten)]
                    inner_one: InnerColumnsOne,
                    a: String,
                    #[scylla(flatten)]
                    inner_two: InnerColumnsTwo,
                }

                let spec = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("x", ColumnType::Native(NativeType::Int)),
                    spec("z", ColumnType::Native(NativeType::Boolean)),
                    spec("y", ColumnType::Native(NativeType::Double)),
                ];

                let value = OuterColumns {
                    inner_one: InnerColumnsOne { x: 5, y: 1.0 },
                    a: "something".to_owned(),
                    inner_two: InnerColumnsTwo { z: true },
                };

                let reference = do_serialize(
                    (
                        &value.a,
                        &value.inner_one.x,
                        &value.inner_two.z,
                        &value.inner_one.y,
                    ),
                    &spec,
                );

                let row = do_serialize(value, &spec);

                assert_eq!(reference, row);
            }

            #[test]
            fn test_flatten_row_serialization_with_enforced_order_and_skip_namecheck() {
                #[derive(SerializeRow, Debug)]
                #[scylla(crate = crate, flavor = "enforce_order")]
                struct OuterColumns {
                    a: String,
                    #[scylla(flatten)]
                    inner_one: InnerColumnsOne,
                    d: i32,
                    #[scylla(flatten)]
                    inner_two: InnerColumnsTwo,
                }

                #[derive(SerializeRow, Debug)]
                #[scylla(crate = crate, flavor = "enforce_order", skip_name_checks)]
                struct InnerColumnsOne {
                    potato: bool,
                    carrot: f32,
                }

                #[derive(SerializeRow, Debug)]
                #[scylla(crate = crate, flavor = "enforce_order")]
                struct InnerColumnsTwo {
                    e: String,
                }

                let value = OuterColumns {
                    a: "A".to_owned(),
                    inner_one: InnerColumnsOne {
                        potato: false,
                        carrot: 2.3,
                    },
                    d: 32,
                    inner_two: InnerColumnsTwo { e: "E".to_owned() },
                };

                let spec = [
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Boolean)),
                    spec("c", ColumnType::Native(NativeType::Float)),
                    spec("d", ColumnType::Native(NativeType::Int)),
                    spec("e", ColumnType::Native(NativeType::Text)),
                ];

                let reference = do_serialize(
                    (
                        &value.a,
                        &value.inner_one.potato,
                        &value.inner_one.carrot,
                        &value.d,
                        &value.inner_two.e,
                    ),
                    &spec,
                );
                let row = do_serialize(value, &spec);

                assert_eq!(reference, row);
            }

            #[test]
            fn test_name_flatten_with_lifetimes() {
                #[derive(SerializeRow)]
                #[scylla(crate = crate, flavor = "enforce_order")]
                struct Inner<'b> {
                    b: &'b bool,
                }

                #[derive(SerializeRow)]
                #[scylla(crate = crate, flavor = "enforce_order")]
                struct Outer<'a, 'b> {
                    #[scylla(flatten)]
                    inner: &'a Inner<'b>,
                }

                let b = true;
                let inner = Inner { b: &b };

                let value = Outer { inner: &inner };
                let spec = [spec("b", ColumnType::Native(NativeType::Boolean))];

                let reference = do_serialize((&value.inner.b,), &spec);
                let row = do_serialize(value, &spec);

                assert_eq!(reference, row);
            }

            #[test]
            fn test_ordered_flatten_with_lifetimes() {
                #[derive(SerializeRow)]
                #[scylla(crate = crate, flavor = "enforce_order")]
                struct Inner<'b> {
                    b: &'b bool,
                }

                #[derive(SerializeRow)]
                #[scylla(crate = crate, flavor = "enforce_order")]
                struct Outer<'a, 'b> {
                    #[scylla(flatten)]
                    inner: &'a Inner<'b>,
                }

                let b = true;
                let inner = Inner { b: &b };

                let value = Outer { inner: &inner };
                let spec = [spec("b", ColumnType::Native(NativeType::Boolean))];

                let reference = do_serialize((&value.inner.b,), &spec);
                let row = do_serialize(value, &spec);

                assert_eq!(reference, row);
            }

            #[test]
            fn test_ordered_flatten_skip_name_check_with_lifetimes() {
                #[derive(SerializeRow)]
                #[scylla(crate = crate, flavor = "enforce_order", skip_name_checks)]
                struct Inner<'b> {
                    potato: &'b bool,
                }

                #[derive(SerializeRow)]
                #[scylla(crate = crate, flavor = "enforce_order", skip_name_checks)]
                struct Outer<'a, 'b> {
                    #[scylla(flatten)]
                    inner: &'a Inner<'b>,
                }

                let potato = true;
                let inner = Inner { potato: &potato };

                let value = Outer { inner: &inner };
                let spec = [spec("b", ColumnType::Native(NativeType::Boolean))];

                let reference = do_serialize((&value.inner.potato,), &spec);
                let row = do_serialize(value, &spec);

                assert_eq!(reference, row);
            }
        }
    }
}
