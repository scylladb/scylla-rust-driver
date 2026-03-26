mod derive_macros_integration {
    mod value {

        use std::borrow::Cow;
        use std::sync::Arc;

        use bytes::Bytes;

        use crate::deserialize::value::{BuiltinDeserializationErrorKind, DeserializeValue};
        use crate::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
        use crate::frame::response::result::{ColumnType, NativeType, UserDefinedType};
        use crate::serialize::SerializationError;
        use crate::serialize::value::SerializeValue;
        use crate::serialize::writers::CellWriter;

        fn udt_def_with_fields(
            fields: impl IntoIterator<Item = (impl Into<Cow<'static, str>>, ColumnType<'static>)>,
        ) -> ColumnType<'static> {
            ColumnType::UserDefinedType {
                frozen: false,
                definition: Arc::new(UserDefinedType {
                    name: "udt".into(),
                    keyspace: "ks".into(),
                    field_types: fields.into_iter().map(|(s, t)| (s.into(), t)).collect(),
                }),
            }
        }

        #[derive(Debug)]
        enum TestDeserializeError {
            TypeCheck(#[expect(dead_code)] TypeCheckError),
            Deserialization(DeserializationError),
        }

        fn deserialize<'frame, 'metadata, T>(
            typ: &'metadata ColumnType<'metadata>,
            bytes: &'frame Bytes,
        ) -> Result<T, TestDeserializeError>
        where
            T: DeserializeValue<'frame, 'metadata>,
        {
            <T as DeserializeValue<'frame, 'metadata>>::type_check(typ)
                .map_err(TestDeserializeError::TypeCheck)?;
            let mut frame_slice = FrameSlice::new(bytes);
            let value = frame_slice.read_cql_bytes().map_err(|err| {
                TestDeserializeError::Deserialization(crate::deserialize::value::mk_deser_err::<T>(
                    typ,
                    BuiltinDeserializationErrorKind::RawCqlBytesReadError(err),
                ))
            })?;
            <T as DeserializeValue<'frame, 'metadata>>::deserialize(typ, value)
                .map_err(TestDeserializeError::Deserialization)
        }

        fn do_serialize_result<T: SerializeValue>(
            t: T,
            typ: &ColumnType,
        ) -> Result<Vec<u8>, SerializationError> {
            let mut ret = Vec::new();
            let writer = CellWriter::new(&mut ret);
            t.serialize(typ, writer).map(|_| ()).map(|()| ret)
        }

        fn do_serialize<T: SerializeValue>(t: T, typ: &ColumnType) -> Vec<u8> {
            do_serialize_result(t, typ).unwrap()
        }

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

        mod serialize {
            use std::sync::Arc;

            use assert_matches::assert_matches;

            use crate::SerializeValue;
            use crate::frame::response::result::{CollectionType, UserDefinedType};
            use crate::frame::response::result::{ColumnType, NativeType};
            use crate::serialize::value::{
                BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
                BuiltinTypeCheckErrorKind, SerializeValue, UdtSerializationErrorKind,
                UdtTypeCheckErrorKind,
            };
            use crate::serialize::writers::CellWriter;
            use crate::value::{Counter, CqlValue};

            use super::do_serialize;

            // Do not remove. It's not used in tests but we keep it here to check that
            // we properly ignore warnings about unused variables, unnecessary `mut`s
            // etc. that usually pop up when generating code for empty structs.
            #[expect(unused)]
            #[derive(SerializeValue)]
            #[scylla(crate = crate)]
            struct TestUdtWithNoFields {}

            #[derive(SerializeValue, Debug, PartialEq, Eq, Default)]
            #[scylla(crate = crate)]
            struct TestUdtWithFieldSorting {
                a: String,
                b: i32,
                c: Vec<i64>,
            }

            #[test]
            fn test_udt_serialization_with_field_sorting_correct_order() {
                let typ = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            (
                                "c".into(),
                                ColumnType::Collection {
                                    frozen: false,
                                    typ: CollectionType::List(Box::new(ColumnType::Native(
                                        NativeType::BigInt,
                                    ))),
                                },
                            ),
                        ],
                    }),
                };

                let reference = do_serialize(
                    CqlValue::UserDefinedType {
                        keyspace: "ks".to_string(),
                        name: "typ".to_string(),
                        fields: vec![
                            (
                                "a".to_string(),
                                Some(CqlValue::Text(String::from("Ala ma kota"))),
                            ),
                            ("b".to_string(), Some(CqlValue::Int(42))),
                            (
                                "c".to_string(),
                                Some(CqlValue::List(vec![
                                    CqlValue::BigInt(1),
                                    CqlValue::BigInt(2),
                                    CqlValue::BigInt(3),
                                ])),
                            ),
                        ],
                    },
                    &typ,
                );
                let udt = do_serialize(
                    TestUdtWithFieldSorting {
                        a: "Ala ma kota".to_owned(),
                        b: 42,
                        c: vec![1, 2, 3],
                    },
                    &typ,
                );

                assert_eq!(reference, udt);
            }

            #[test]
            fn test_udt_serialization_with_field_sorting_incorrect_order() {
                let typ = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            // Two first columns are swapped
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            (
                                "c".into(),
                                ColumnType::Collection {
                                    frozen: false,
                                    typ: CollectionType::List(Box::new(ColumnType::Native(
                                        NativeType::BigInt,
                                    ))),
                                },
                            ),
                        ],
                    }),
                };

                let reference = do_serialize(
                    CqlValue::UserDefinedType {
                        keyspace: "ks".to_string(),
                        name: "typ".to_string(),
                        fields: vec![
                            // FIXME: UDTs in CqlValue should also honor the order
                            // For now, it's swapped here as well
                            ("b".to_string(), Some(CqlValue::Int(42))),
                            (
                                "a".to_string(),
                                Some(CqlValue::Text(String::from("Ala ma kota"))),
                            ),
                            (
                                "c".to_string(),
                                Some(CqlValue::List(vec![
                                    CqlValue::BigInt(1),
                                    CqlValue::BigInt(2),
                                    CqlValue::BigInt(3),
                                ])),
                            ),
                        ],
                    },
                    &typ,
                );
                let udt = do_serialize(
                    TestUdtWithFieldSorting {
                        a: "Ala ma kota".to_owned(),
                        b: 42,
                        c: vec![1, 2, 3],
                    },
                    &typ,
                );

                assert_eq!(reference, udt);
            }

            #[test]
            fn test_udt_serialization_with_missing_rust_fields_at_end() {
                let udt = TestUdtWithFieldSorting::default();

                let typ_normal = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            (
                                "c".into(),
                                ColumnType::Collection {
                                    frozen: false,
                                    typ: CollectionType::List(Box::new(ColumnType::Native(
                                        NativeType::BigInt,
                                    ))),
                                },
                            ),
                        ],
                    }),
                };

                let typ_unexpected_field = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            (
                                "c".into(),
                                ColumnType::Collection {
                                    frozen: false,
                                    typ: CollectionType::List(Box::new(ColumnType::Native(
                                        NativeType::BigInt,
                                    ))),
                                },
                            ),
                            // Unexpected fields
                            ("d".into(), ColumnType::Native(NativeType::Counter)),
                            ("e".into(), ColumnType::Native(NativeType::Counter)),
                        ],
                    }),
                };

                let result_normal = do_serialize(&udt, &typ_normal);
                let result_additional_field = do_serialize(&udt, &typ_unexpected_field);

                assert_eq!(result_normal, result_additional_field);
            }

            #[derive(SerializeValue, Debug, PartialEq, Default)]
            #[scylla(crate = crate)]
            struct TestUdtWithFieldSorting2 {
                a: String,
                b: i32,
                d: Option<Counter>,
                c: Vec<i64>,
            }

            #[derive(SerializeValue, Debug, PartialEq, Default)]
            #[scylla(crate = crate)]
            struct TestUdtWithFieldSorting3 {
                a: String,
                b: i32,
                d: Option<Counter>,
                e: Option<f32>,
                c: Vec<i64>,
            }

            #[test]
            fn test_udt_serialization_with_missing_rust_field_in_middle() {
                let udt = TestUdtWithFieldSorting::default();
                let udt2 = TestUdtWithFieldSorting2::default();
                let udt3 = TestUdtWithFieldSorting3::default();

                let typ = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            // Unexpected fields
                            ("d".into(), ColumnType::Native(NativeType::Counter)),
                            ("e".into(), ColumnType::Native(NativeType::Float)),
                            // Remaining normal field
                            (
                                "c".into(),
                                ColumnType::Collection {
                                    frozen: false,
                                    typ: CollectionType::List(Box::new(ColumnType::Native(
                                        NativeType::BigInt,
                                    ))),
                                },
                            ),
                        ],
                    }),
                };

                let result_1 = do_serialize(udt, &typ);
                let result_2 = do_serialize(udt2, &typ);
                let result_3 = do_serialize(udt3, &typ);

                assert_eq!(result_1, result_2);
                assert_eq!(result_2, result_3);
            }

            #[test]
            fn test_udt_serialization_failing_type_check() {
                let typ_not_udt = ColumnType::Native(NativeType::Ascii);
                let udt = TestUdtWithFieldSorting::default();
                let mut data = Vec::new();

                let err = udt
                    .serialize(&typ_not_udt, CellWriter::new(&mut data))
                    .unwrap_err();
                let err = err.downcast_ref::<BuiltinTypeCheckError>().unwrap();
                assert_matches!(
                    err.kind,
                    BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NotUdt)
                );

                let typ_without_c = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            // Last field is missing
                        ],
                    }),
                };

                let err = udt
                    .serialize(&typ_without_c, CellWriter::new(&mut data))
                    .unwrap_err();
                let err = err.downcast_ref::<BuiltinTypeCheckError>().unwrap();
                assert_matches!(
                    err.kind,
                    BuiltinTypeCheckErrorKind::UdtError(
                        UdtTypeCheckErrorKind::ValueMissingForUdtField { .. }
                    )
                );

                let typ_wrong_type = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            ("c".into(), ColumnType::Native(NativeType::TinyInt)), // Wrong column type
                        ],
                    }),
                };

                let err = udt
                    .serialize(&typ_wrong_type, CellWriter::new(&mut data))
                    .unwrap_err();
                let err = err.downcast_ref::<BuiltinSerializationError>().unwrap();
                assert_matches!(
                    err.kind,
                    BuiltinSerializationErrorKind::UdtError(
                        UdtSerializationErrorKind::FieldSerializationFailed { .. }
                    )
                );
            }

            #[derive(SerializeValue)]
            #[scylla(crate = crate)]
            struct TestUdtWithGenerics<'a, T: SerializeValue> {
                a: &'a str,
                b: T,
            }

            #[test]
            fn test_udt_serialization_with_generics() {
                // A minimal smoke test just to test that it works.
                fn check_with_type<T: SerializeValue>(typ: ColumnType, t: T, cql_t: CqlValue) {
                    let typ = ColumnType::UserDefinedType {
                        frozen: false,
                        definition: Arc::new(UserDefinedType {
                            name: "typ".into(),
                            keyspace: "ks".into(),
                            field_types: vec![
                                ("a".into(), ColumnType::Native(NativeType::Text)),
                                ("b".into(), typ),
                            ],
                        }),
                    };
                    let reference = do_serialize(
                        CqlValue::UserDefinedType {
                            keyspace: "ks".to_string(),
                            name: "typ".to_string(),
                            fields: vec![
                                (
                                    "a".to_string(),
                                    Some(CqlValue::Text(String::from("Ala ma kota"))),
                                ),
                                ("b".to_string(), Some(cql_t)),
                            ],
                        },
                        &typ,
                    );
                    let udt = do_serialize(
                        TestUdtWithGenerics {
                            a: "Ala ma kota",
                            b: t,
                        },
                        &typ,
                    );
                    assert_eq!(reference, udt);
                }

                check_with_type(
                    ColumnType::Native(NativeType::Int),
                    123_i32,
                    CqlValue::Int(123_i32),
                );
                check_with_type(
                    ColumnType::Native(NativeType::Double),
                    123_f64,
                    CqlValue::Double(123_f64),
                );
            }

            #[derive(SerializeValue, Debug, PartialEq, Eq, Default)]
            #[scylla(crate = crate, flavor = "enforce_order")]
            struct TestUdtWithEnforcedOrder {
                a: String,
                b: i32,
                c: Vec<i64>,
            }

            #[test]
            fn test_udt_serialization_with_enforced_order_correct_order() {
                let typ = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            (
                                "c".into(),
                                ColumnType::Collection {
                                    frozen: false,
                                    typ: CollectionType::List(Box::new(ColumnType::Native(
                                        NativeType::BigInt,
                                    ))),
                                },
                            ),
                        ],
                    }),
                };

                let reference = do_serialize(
                    CqlValue::UserDefinedType {
                        keyspace: "ks".to_string(),
                        name: "typ".to_string(),
                        fields: vec![
                            (
                                "a".to_string(),
                                Some(CqlValue::Text(String::from("Ala ma kota"))),
                            ),
                            ("b".to_string(), Some(CqlValue::Int(42))),
                            (
                                "c".to_string(),
                                Some(CqlValue::List(vec![
                                    CqlValue::BigInt(1),
                                    CqlValue::BigInt(2),
                                    CqlValue::BigInt(3),
                                ])),
                            ),
                        ],
                    },
                    &typ,
                );
                let udt = do_serialize(
                    TestUdtWithEnforcedOrder {
                        a: "Ala ma kota".to_owned(),
                        b: 42,
                        c: vec![1, 2, 3],
                    },
                    &typ,
                );

                assert_eq!(reference, udt);
            }

            #[test]
            fn test_udt_serialization_with_enforced_order_additional_field() {
                let udt = TestUdtWithEnforcedOrder::default();

                let typ_normal = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            (
                                "c".into(),
                                ColumnType::Collection {
                                    frozen: false,
                                    typ: CollectionType::List(Box::new(ColumnType::Native(
                                        NativeType::BigInt,
                                    ))),
                                },
                            ),
                        ],
                    }),
                };

                let typ_unexpected_field = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            (
                                "c".into(),
                                ColumnType::Collection {
                                    frozen: false,
                                    typ: CollectionType::List(Box::new(ColumnType::Native(
                                        NativeType::BigInt,
                                    ))),
                                },
                            ),
                            // Unexpected field
                            ("d".into(), ColumnType::Native(NativeType::Counter)),
                        ],
                    }),
                };

                let result_normal = do_serialize(&udt, &typ_normal);
                let result_additional_field = do_serialize(&udt, &typ_unexpected_field);

                assert_eq!(result_normal, result_additional_field);
            }

            #[test]
            fn test_udt_serialization_with_enforced_order_failing_type_check() {
                let typ_not_udt = ColumnType::Native(NativeType::Ascii);
                let udt = TestUdtWithEnforcedOrder::default();

                let mut data = Vec::new();

                let err = <_ as SerializeValue>::serialize(
                    &udt,
                    &typ_not_udt,
                    CellWriter::new(&mut data),
                )
                .unwrap_err();
                let err = err.downcast_ref::<BuiltinTypeCheckError>().unwrap();
                assert_matches!(
                    err.kind,
                    BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NotUdt)
                );

                let typ = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            // Two first columns are swapped
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            (
                                "c".into(),
                                ColumnType::Collection {
                                    frozen: false,
                                    typ: CollectionType::List(Box::new(ColumnType::Native(
                                        NativeType::BigInt,
                                    ))),
                                },
                            ),
                        ],
                    }),
                };

                let err = <_ as SerializeValue>::serialize(&udt, &typ, CellWriter::new(&mut data))
                    .unwrap_err();
                let err = err.downcast_ref::<BuiltinTypeCheckError>().unwrap();
                assert_matches!(
                    err.kind,
                    BuiltinTypeCheckErrorKind::UdtError(
                        UdtTypeCheckErrorKind::FieldNameMismatch { .. }
                    )
                );

                let typ_without_c = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            // Last field is missing
                        ],
                    }),
                };

                let err = <_ as SerializeValue>::serialize(
                    &udt,
                    &typ_without_c,
                    CellWriter::new(&mut data),
                )
                .unwrap_err();
                let err = err.downcast_ref::<BuiltinTypeCheckError>().unwrap();
                assert_matches!(
                    err.kind,
                    BuiltinTypeCheckErrorKind::UdtError(
                        UdtTypeCheckErrorKind::ValueMissingForUdtField { .. }
                    )
                );

                let typ_unexpected_field = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            ("c".into(), ColumnType::Native(NativeType::TinyInt)), // Wrong column type
                        ],
                    }),
                };

                let err = <_ as SerializeValue>::serialize(
                    &udt,
                    &typ_unexpected_field,
                    CellWriter::new(&mut data),
                )
                .unwrap_err();
                let err = err.downcast_ref::<BuiltinSerializationError>().unwrap();
                assert_matches!(
                    err.kind,
                    BuiltinSerializationErrorKind::UdtError(
                        UdtSerializationErrorKind::FieldSerializationFailed { .. }
                    )
                );
            }

            #[derive(SerializeValue, Debug)]
            #[scylla(crate = crate)]
            struct TestUdtWithFieldRename {
                a: String,
                #[scylla(rename = "x")]
                b: i32,
            }

            #[derive(SerializeValue, Debug)]
            #[scylla(crate = crate, flavor = "enforce_order")]
            struct TestUdtWithFieldRenameAndEnforceOrder {
                a: String,
                #[scylla(rename = "x")]
                b: i32,
            }

            #[test]
            fn test_udt_serialization_with_field_rename() {
                let typ = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("x".into(), ColumnType::Native(NativeType::Int)),
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                        ],
                    }),
                };

                let mut reference = Vec::new();
                // Total length of the struct is 23
                reference.extend_from_slice(&23i32.to_be_bytes());
                // Field 'x'
                reference.extend_from_slice(&4i32.to_be_bytes());
                reference.extend_from_slice(&42i32.to_be_bytes());
                // Field 'a'
                reference.extend_from_slice(&("Ala ma kota".len() as i32).to_be_bytes());
                reference.extend_from_slice("Ala ma kota".as_bytes());

                let udt = do_serialize(
                    TestUdtWithFieldRename {
                        a: "Ala ma kota".to_owned(),
                        b: 42,
                    },
                    &typ,
                );

                assert_eq!(reference, udt);
            }

            #[test]
            fn test_udt_serialization_with_field_rename_and_enforce_order() {
                let typ = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("x".into(), ColumnType::Native(NativeType::Int)),
                        ],
                    }),
                };

                let mut reference = Vec::new();
                // Total length of the struct is 23
                reference.extend_from_slice(&23i32.to_be_bytes());
                // Field 'a'
                reference.extend_from_slice(&("Ala ma kota".len() as i32).to_be_bytes());
                reference.extend_from_slice("Ala ma kota".as_bytes());
                // Field 'x'
                reference.extend_from_slice(&4i32.to_be_bytes());
                reference.extend_from_slice(&42i32.to_be_bytes());

                let udt = do_serialize(
                    TestUdtWithFieldRenameAndEnforceOrder {
                        a: "Ala ma kota".to_owned(),
                        b: 42,
                    },
                    &typ,
                );

                assert_eq!(reference, udt);
            }

            #[expect(unused)]
            #[derive(SerializeValue, Debug)]
            #[scylla(crate = crate, flavor = "enforce_order", skip_name_checks)]
            struct TestUdtWithSkippedNameChecks {
                a: String,
                b: i32,
            }

            #[test]
            fn test_udt_serialization_with_skipped_name_checks() {
                let typ = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("x".into(), ColumnType::Native(NativeType::Int)),
                        ],
                    }),
                };

                let mut reference = Vec::new();
                // Total length of the struct is 23
                reference.extend_from_slice(&23i32.to_be_bytes());
                // Field 'a'
                reference.extend_from_slice(&("Ala ma kota".len() as i32).to_be_bytes());
                reference.extend_from_slice("Ala ma kota".as_bytes());
                // Field 'x'
                reference.extend_from_slice(&4i32.to_be_bytes());
                reference.extend_from_slice(&42i32.to_be_bytes());

                let udt = do_serialize(
                    TestUdtWithFieldRenameAndEnforceOrder {
                        a: "Ala ma kota".to_owned(),
                        b: 42,
                    },
                    &typ,
                );

                assert_eq!(reference, udt);
            }

            #[derive(SerializeValue, Debug, PartialEq, Eq, Default)]
            #[scylla(crate = crate, forbid_excess_udt_fields)]
            struct TestStrictUdtWithFieldSorting {
                a: String,
                b: i32,
                c: Vec<i64>,
            }

            #[test]
            fn test_strict_udt_with_field_sorting_rejects_additional_field() {
                let udt = TestStrictUdtWithFieldSorting::default();
                let mut data = Vec::new();

                let typ_unexpected_field = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            (
                                "c".into(),
                                ColumnType::Collection {
                                    frozen: false,
                                    typ: CollectionType::List(Box::new(ColumnType::Native(
                                        NativeType::BigInt,
                                    ))),
                                },
                            ),
                            // Unexpected field
                            ("d".into(), ColumnType::Native(NativeType::Counter)),
                        ],
                    }),
                };

                let err = udt
                    .serialize(&typ_unexpected_field, CellWriter::new(&mut data))
                    .unwrap_err();
                let err = err.downcast_ref::<BuiltinTypeCheckError>().unwrap();
                assert_matches!(
                    err.kind,
                    BuiltinTypeCheckErrorKind::UdtError(
                        UdtTypeCheckErrorKind::NoSuchFieldInUdt { .. }
                    )
                );

                let typ_unexpected_field_middle = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            // Unexpected field
                            ("b_c".into(), ColumnType::Native(NativeType::Counter)),
                            (
                                "c".into(),
                                ColumnType::Collection {
                                    frozen: false,
                                    typ: CollectionType::List(Box::new(ColumnType::Native(
                                        NativeType::BigInt,
                                    ))),
                                },
                            ),
                        ],
                    }),
                };

                let err = udt
                    .serialize(&typ_unexpected_field_middle, CellWriter::new(&mut data))
                    .unwrap_err();
                let err = err.downcast_ref::<BuiltinTypeCheckError>().unwrap();
                assert_matches!(
                    err.kind,
                    BuiltinTypeCheckErrorKind::UdtError(
                        UdtTypeCheckErrorKind::NoSuchFieldInUdt { .. }
                    )
                );
            }

            #[derive(SerializeValue, Debug, PartialEq, Eq, Default)]
            #[scylla(crate = crate, flavor = "enforce_order", forbid_excess_udt_fields)]
            struct TestStrictUdtWithEnforcedOrder {
                a: String,
                b: i32,
                c: Vec<i64>,
            }

            #[test]
            fn test_strict_udt_with_enforced_order_rejects_additional_field() {
                let udt = TestStrictUdtWithEnforcedOrder::default();
                let mut data = Vec::new();

                let typ_unexpected_field = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            (
                                "c".into(),
                                ColumnType::Collection {
                                    frozen: false,
                                    typ: CollectionType::List(Box::new(ColumnType::Native(
                                        NativeType::BigInt,
                                    ))),
                                },
                            ),
                            // Unexpected field
                            ("d".into(), ColumnType::Native(NativeType::Counter)),
                        ],
                    }),
                };

                let err = <_ as SerializeValue>::serialize(
                    &udt,
                    &typ_unexpected_field,
                    CellWriter::new(&mut data),
                )
                .unwrap_err();
                let err = err.downcast_ref::<BuiltinTypeCheckError>().unwrap();
                assert_matches!(
                    err.kind,
                    BuiltinTypeCheckErrorKind::UdtError(
                        UdtTypeCheckErrorKind::NoSuchFieldInUdt { .. }
                    )
                );
            }

            #[derive(SerializeValue, Debug)]
            #[scylla(crate = crate, flavor = "enforce_order", skip_name_checks)]
            struct TestUdtWithSkippedFields {
                a: String,
                b: i32,
                #[scylla(skip)]
                #[expect(dead_code)]
                skipped: Vec<String>,
                c: Vec<i64>,
            }

            #[test]
            fn test_row_serialization_with_skipped_field() {
                let typ = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![
                            ("a".into(), ColumnType::Native(NativeType::Text)),
                            ("b".into(), ColumnType::Native(NativeType::Int)),
                            (
                                "c".into(),
                                ColumnType::Collection {
                                    frozen: false,
                                    typ: CollectionType::List(Box::new(ColumnType::Native(
                                        NativeType::BigInt,
                                    ))),
                                },
                            ),
                        ],
                    }),
                };

                let reference = do_serialize(
                    TestUdtWithFieldSorting {
                        a: "Ala ma kota".to_owned(),
                        b: 42,
                        c: vec![1, 2, 3],
                    },
                    &typ,
                );
                let row = do_serialize(
                    TestUdtWithSkippedFields {
                        a: "Ala ma kota".to_owned(),
                        b: 42,
                        skipped: vec!["abcd".to_owned(), "efgh".to_owned()],
                        c: vec![1, 2, 3],
                    },
                    &typ,
                );

                assert_eq!(reference, row);
            }

            #[test]
            fn test_udt_with_non_rust_ident() {
                #[derive(SerializeValue, Debug)]
                #[scylla(crate = crate)]
                struct UdtWithNonRustIdent {
                    #[scylla(rename = "a$a")]
                    a: i32,
                }

                let typ = ColumnType::UserDefinedType {
                    frozen: false,
                    definition: Arc::new(UserDefinedType {
                        name: "typ".into(),
                        keyspace: "ks".into(),
                        field_types: vec![("a$a".into(), ColumnType::Native(NativeType::Int))],
                    }),
                };
                let value = UdtWithNonRustIdent { a: 42 };

                let mut reference = Vec::new();
                // Total length of the struct
                reference.extend_from_slice(&8i32.to_be_bytes());
                // Field 'a'
                reference
                    .extend_from_slice(&(std::mem::size_of_val(&value.a) as i32).to_be_bytes());
                reference.extend_from_slice(&value.a.to_be_bytes());

                let udt = do_serialize(value, &typ);

                assert_eq!(reference, udt);
            }
        }

        mod deserialize {
            use std::sync::Arc;

            use assert_matches::assert_matches;
            use bytes::{BufMut, Bytes, BytesMut};

            use super::{TestDeserializeError, deserialize, udt_def_with_fields};
            use crate::DeserializeValue;
            use crate::deserialize::value::{
                BuiltinDeserializationError, BuiltinDeserializationErrorKind,
                BuiltinTypeCheckError, BuiltinTypeCheckErrorKind, DeserializeValue,
                UdtDeserializationErrorKind, UdtTypeCheckErrorKind,
            };
            use crate::deserialize::{DeserializationError, TypeCheckError};
            use crate::frame::response::result::{
                CollectionType, ColumnType, NativeType, UserDefinedType,
            };

            fn append_bytes(b: &mut impl BufMut, cell: &[u8]) {
                b.put_i32(cell.len() as i32);
                b.put_slice(cell);
            }

            fn append_null(b: &mut impl BufMut) {
                b.put_i32(-1);
            }

            fn make_bytes(cell: &[u8]) -> Bytes {
                let mut b = BytesMut::new();
                append_bytes(&mut b, cell);
                b.freeze()
            }

            #[must_use]
            struct UdtSerializer {
                buf: BytesMut,
            }

            impl UdtSerializer {
                fn new() -> Self {
                    Self {
                        buf: BytesMut::default(),
                    }
                }

                fn field(mut self, field_bytes: &[u8]) -> Self {
                    append_bytes(&mut self.buf, field_bytes);
                    self
                }

                fn null_field(mut self) -> Self {
                    append_null(&mut self.buf);
                    self
                }

                fn finalize(&self) -> Bytes {
                    make_bytes(&self.buf)
                }
            }

            #[track_caller]
            fn get_typeck_err_inner(err: &TypeCheckError) -> &BuiltinTypeCheckError {
                match err.downcast_ref() {
                    Some(err) => err,
                    None => panic!("not a BuiltinTypeCheckError: {err:?}"),
                }
            }

            #[track_caller]
            fn get_deser_err_inner(err: &DeserializationError) -> &BuiltinDeserializationError {
                match err.downcast_ref() {
                    Some(err) => err,
                    None => panic!("not a BuiltinDeserializationError: {err:?}"),
                }
            }

            #[track_caller]
            fn get_deser_err(err: &TestDeserializeError) -> &BuiltinDeserializationError {
                match err {
                    TestDeserializeError::Deserialization(err) => get_deser_err_inner(err),
                    other => panic!("expected Deserialization error, got: {other:?}"),
                }
            }

            // Do not remove. It's not used in tests but we keep it here to check that
            // we properly ignore warnings about unused variables, unnecessary `mut`s
            // etc. that usually pop up when generating code for empty structs.
            #[derive(scylla_macros::DeserializeValue)]
            #[scylla(crate = crate)]
            struct TestUdtWithNoFieldsUnordered {}

            #[derive(scylla_macros::DeserializeValue)]
            #[scylla(crate = crate, flavor = "enforce_order")]
            struct TestUdtWithNoFieldsOrdered {}

            // If deserialize is never called, rust warns that the struct is never constructed.
            // We don't want to `expect(dead_code)` on struct definitions, because that could silence
            // some warnings that this test is supposed to prevent.
            #[expect(unreachable_code, dead_code)]
            fn dummy_deserialize_udts() {
                let _ = deserialize::<TestUdtWithNoFieldsUnordered>(todo!(), todo!()).unwrap();
                let _ = deserialize::<TestUdtWithNoFieldsOrdered>(todo!(), todo!()).unwrap();
            }

            #[test]
            fn test_udt_loose_ordering() {
                #[derive(scylla_macros::DeserializeValue, PartialEq, Eq, Debug)]
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

                // UDT fields in correct same order.
                {
                    let udt_bytes = UdtSerializer::new()
                        .field("The quick brown fox".as_bytes())
                        .field(&42_i32.to_be_bytes())
                        .field(&2137_i64.to_be_bytes())
                        .finalize();
                    let typ = udt_def_with_fields([
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("b", ColumnType::Native(NativeType::Int)),
                        ("c", ColumnType::Native(NativeType::BigInt)),
                    ]);

                    let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
                    assert_eq!(
                        udt,
                        Udt {
                            a: "The quick brown fox",
                            x: String::new(),
                            b: Some(42),
                            c: 2137,
                        }
                    );
                }

                // The last two UDT field are missing in serialized form - it should treat it
                // as if there were nulls at the end.
                {
                    let udt_bytes = UdtSerializer::new()
                        .field("The quick brown fox".as_bytes())
                        .finalize();
                    let typ = udt_def_with_fields([
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("b", ColumnType::Native(NativeType::Int)),
                        ("c", ColumnType::Native(NativeType::BigInt)),
                    ]);

                    let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
                    assert_eq!(
                        udt,
                        Udt {
                            a: "The quick brown fox",
                            x: String::new(),
                            b: None,
                            c: 0,
                        }
                    );
                }

                // UDT fields switched - should still work.
                {
                    let udt_bytes = UdtSerializer::new()
                        .field(&42_i32.to_be_bytes())
                        .field("The quick brown fox".as_bytes())
                        .field(&2137_i64.to_be_bytes())
                        .finalize();
                    let typ = udt_def_with_fields([
                        ("b", ColumnType::Native(NativeType::Int)),
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("c", ColumnType::Native(NativeType::BigInt)),
                    ]);

                    let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
                    assert_eq!(
                        udt,
                        Udt {
                            a: "The quick brown fox",
                            x: String::new(),
                            b: Some(42),
                            c: 2137,
                        }
                    );
                }

                // An excess UDT field - should still work.
                {
                    let udt_bytes = UdtSerializer::new()
                        .field(&12_i8.to_be_bytes())
                        .field(&42_i32.to_be_bytes())
                        .field("The quick brown fox".as_bytes())
                        .field(&2137_i64.to_be_bytes())
                        .finalize();
                    let typ = udt_def_with_fields([
                        ("d", ColumnType::Native(NativeType::TinyInt)),
                        ("b", ColumnType::Native(NativeType::Int)),
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("c", ColumnType::Native(NativeType::BigInt)),
                    ]);

                    Udt::type_check(&typ).unwrap();
                    let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
                    assert_eq!(
                        udt,
                        Udt {
                            a: "The quick brown fox",
                            x: String::new(),
                            b: Some(42),
                            c: 2137,
                        }
                    );
                }

                // Only field 'a' is present
                {
                    let udt_bytes = UdtSerializer::new()
                        .field("The quick brown fox".as_bytes())
                        .finalize();
                    let typ = udt_def_with_fields([
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("c", ColumnType::Native(NativeType::BigInt)),
                    ]);

                    let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
                    assert_eq!(
                        udt,
                        Udt {
                            a: "The quick brown fox",
                            x: String::new(),
                            b: None,
                            c: 0,
                        }
                    );
                }

                // Wrong column type
                {
                    let typ = udt_def_with_fields([("a", ColumnType::Native(NativeType::Text))]);
                    Udt::type_check(&typ).unwrap_err();
                }

                // Missing required column
                {
                    let typ = udt_def_with_fields([("b", ColumnType::Native(NativeType::Int))]);
                    Udt::type_check(&typ).unwrap_err();
                }
            }

            #[test]
            fn test_udt_strict_ordering() {
                #[derive(scylla_macros::DeserializeValue, PartialEq, Eq, Debug)]
                #[scylla(crate = "crate", flavor = "enforce_order")]
                struct Udt<'a> {
                    #[scylla(default_when_null)]
                    a: &'a str,
                    #[scylla(skip)]
                    x: String,
                    #[scylla(allow_missing)]
                    b: Option<i32>,
                }

                // UDT fields in correct same order
                {
                    let udt_bytes = UdtSerializer::new()
                        .field("The quick brown fox".as_bytes())
                        .field(&42i32.to_be_bytes())
                        .finalize();
                    let typ = udt_def_with_fields([
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("b", ColumnType::Native(NativeType::Int)),
                    ]);

                    let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
                    assert_eq!(
                        udt,
                        Udt {
                            a: "The quick brown fox",
                            x: String::new(),
                            b: Some(42),
                        }
                    );
                }

                // The last UDT field is missing in serialized form - it should treat
                // as if there were null at the end
                {
                    let udt_bytes = UdtSerializer::new()
                        .field("The quick brown fox".as_bytes())
                        .finalize();
                    let typ = udt_def_with_fields([
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("b", ColumnType::Native(NativeType::Int)),
                    ]);

                    let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
                    assert_eq!(
                        udt,
                        Udt {
                            a: "The quick brown fox",
                            x: String::new(),
                            b: None,
                        }
                    );
                }

                // An excess field at the end of UDT
                {
                    let udt_bytes = UdtSerializer::new()
                        .field("The quick brown fox".as_bytes())
                        .field(&42_i32.to_be_bytes())
                        .field(&(true as i8).to_be_bytes())
                        .finalize();
                    let typ = udt_def_with_fields([
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("b", ColumnType::Native(NativeType::Int)),
                        ("d", ColumnType::Native(NativeType::Boolean)),
                    ]);
                    let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
                    assert_eq!(
                        udt,
                        Udt {
                            a: "The quick brown fox",
                            x: String::new(),
                            b: Some(42),
                        }
                    );
                }

                // An excess field at the end of UDT, when such are forbidden
                {
                    #[derive(scylla_macros::DeserializeValue, PartialEq, Eq, Debug)]
                    #[scylla(crate = "crate", flavor = "enforce_order", forbid_excess_udt_fields)]
                    struct Udt<'a> {
                        a: &'a str,
                        #[scylla(skip)]
                        x: String,
                        b: Option<i32>,
                    }

                    let typ = udt_def_with_fields([
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("b", ColumnType::Native(NativeType::Int)),
                        ("d", ColumnType::Native(NativeType::Boolean)),
                    ]);

                    Udt::type_check(&typ).unwrap_err();
                }

                // UDT fields switched - will not work
                {
                    let typ = udt_def_with_fields([
                        ("b", ColumnType::Native(NativeType::Int)),
                        ("a", ColumnType::Native(NativeType::Text)),
                    ]);
                    Udt::type_check(&typ).unwrap_err();
                }

                // Wrong column type
                {
                    let typ = udt_def_with_fields([
                        ("a", ColumnType::Native(NativeType::Int)),
                        ("b", ColumnType::Native(NativeType::Int)),
                    ]);
                    Udt::type_check(&typ).unwrap_err();
                }

                // Missing required column
                {
                    let typ = udt_def_with_fields([("b", ColumnType::Native(NativeType::Int))]);
                    Udt::type_check(&typ).unwrap_err();
                }

                // Missing non-required column
                {
                    let udt_bytes = UdtSerializer::new().field(b"kotmaale").finalize();
                    let typ = udt_def_with_fields([("a", ColumnType::Native(NativeType::Text))]);

                    let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
                    assert_eq!(
                        udt,
                        Udt {
                            a: "kotmaale",
                            x: String::new(),
                            b: None,
                        }
                    );
                }

                // The first field is null, but `default_when_null` prevents failure.
                {
                    let udt_bytes = UdtSerializer::new()
                        .null_field()
                        .field(&42i32.to_be_bytes())
                        .finalize();
                    let typ = udt_def_with_fields([
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("b", ColumnType::Native(NativeType::Int)),
                    ]);

                    let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
                    assert_eq!(
                        udt,
                        Udt {
                            a: "",
                            x: String::new(),
                            b: Some(42),
                        }
                    );
                }
            }

            #[test]
            fn test_udt_no_name_check() {
                #[derive(scylla_macros::DeserializeValue, PartialEq, Eq, Debug)]
                #[scylla(crate = "crate", flavor = "enforce_order", skip_name_checks)]
                struct Udt<'a> {
                    a: &'a str,
                    #[scylla(skip)]
                    x: String,
                    b: Option<i32>,
                }

                // UDT fields in correct same order
                {
                    let udt_bytes = UdtSerializer::new()
                        .field("The quick brown fox".as_bytes())
                        .field(&42i32.to_be_bytes())
                        .finalize();
                    let typ = udt_def_with_fields([
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("b", ColumnType::Native(NativeType::Int)),
                    ]);

                    let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
                    assert_eq!(
                        udt,
                        Udt {
                            a: "The quick brown fox",
                            x: String::new(),
                            b: Some(42),
                        }
                    );
                }

                // Correct order of UDT fields, but different names - should still succeed
                {
                    let udt_bytes = UdtSerializer::new()
                        .field("The quick brown fox".as_bytes())
                        .field(&42i32.to_be_bytes())
                        .finalize();
                    let typ = udt_def_with_fields([
                        ("k", ColumnType::Native(NativeType::Text)),
                        ("l", ColumnType::Native(NativeType::Int)),
                    ]);

                    let udt = deserialize::<Udt<'_>>(&typ, &udt_bytes).unwrap();
                    assert_eq!(
                        udt,
                        Udt {
                            a: "The quick brown fox",
                            x: String::new(),
                            b: Some(42),
                        }
                    );
                }
            }

            #[test]
            fn test_udt_cross_rename_fields() {
                #[derive(scylla_macros::DeserializeValue, PartialEq, Eq, Debug)]
                #[scylla(crate = crate)]
                struct TestUdt {
                    #[scylla(rename = "b")]
                    a: i32,
                    #[scylla(rename = "a")]
                    b: String,
                }

                // UDT fields switched - should still work.
                {
                    let udt_bytes = UdtSerializer::new()
                        .field("The quick brown fox".as_bytes())
                        .field(&42_i32.to_be_bytes())
                        .finalize();
                    let typ = udt_def_with_fields([
                        ("a", ColumnType::Native(NativeType::Text)),
                        ("b", ColumnType::Native(NativeType::Int)),
                    ]);

                    let udt = deserialize::<TestUdt>(&typ, &udt_bytes).unwrap();
                    assert_eq!(
                        udt,
                        TestUdt {
                            a: 42,
                            b: "The quick brown fox".to_owned(),
                        }
                    );
                }
            }

            #[test]
            fn test_udt_errors() {
                // Loose ordering
                {
                    #[derive(scylla_macros::DeserializeValue, PartialEq, Eq, Debug)]
                    #[scylla(crate = "crate", forbid_excess_udt_fields)]
                    struct Udt<'a> {
                        a: &'a str,
                        #[scylla(skip)]
                        x: String,
                        #[scylla(allow_missing)]
                        b: Option<i32>,
                        #[scylla(default_when_null)]
                        c: bool,
                    }

                    // Type check errors
                    {
                        // Not UDT
                        {
                            let typ = ColumnType::Collection {
                                frozen: false,
                                typ: CollectionType::Map(
                                    Box::new(ColumnType::Native(NativeType::Ascii)),
                                    Box::new(ColumnType::Native(NativeType::Blob)),
                                ),
                            };
                            let err = Udt::type_check(&typ).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                            assert_eq!(err.cql_type, typ);
                            let BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NotUdt) =
                                err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                        }

                        // UDT missing fields
                        {
                            let typ = udt_def_with_fields([(
                                "c",
                                ColumnType::Native(NativeType::Boolean),
                            )]);
                            let err = Udt::type_check(&typ).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                            assert_eq!(err.cql_type, typ);
                            let BuiltinTypeCheckErrorKind::UdtError(
                                UdtTypeCheckErrorKind::ValuesMissingForUdtFields {
                                    field_names: ref missing_fields,
                                },
                            ) = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(missing_fields.as_slice(), &["a"]);
                        }

                        // excess fields in UDT
                        {
                            let typ = udt_def_with_fields([
                                ("d", ColumnType::Native(NativeType::Boolean)),
                                ("a", ColumnType::Native(NativeType::Text)),
                                ("b", ColumnType::Native(NativeType::Int)),
                            ]);
                            let err = Udt::type_check(&typ).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                            assert_eq!(err.cql_type, typ);
                            let BuiltinTypeCheckErrorKind::UdtError(
                                UdtTypeCheckErrorKind::ExcessFieldInUdt { ref db_field_name },
                            ) = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(db_field_name.as_str(), "d");
                        }

                        // missing UDT field
                        {
                            let typ = udt_def_with_fields([
                                ("b", ColumnType::Native(NativeType::Int)),
                                ("a", ColumnType::Native(NativeType::Text)),
                            ]);
                            let err = Udt::type_check(&typ).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                            assert_eq!(err.cql_type, typ);
                            let BuiltinTypeCheckErrorKind::UdtError(
                                UdtTypeCheckErrorKind::ValuesMissingForUdtFields {
                                    ref field_names,
                                },
                            ) = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(field_names, &["c"]);
                        }

                        // UDT fields incompatible types - field type check failed
                        {
                            let typ = udt_def_with_fields([
                                ("a", ColumnType::Native(NativeType::Blob)),
                                ("b", ColumnType::Native(NativeType::Int)),
                            ]);
                            let err = Udt::type_check(&typ).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                            assert_eq!(err.cql_type, typ);
                            let BuiltinTypeCheckErrorKind::UdtError(
                                UdtTypeCheckErrorKind::FieldTypeCheckFailed {
                                    ref field_name,
                                    ref err,
                                },
                            ) = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(field_name.as_str(), "a");
                            let err = get_typeck_err_inner(err);
                            assert_eq!(err.rust_name, std::any::type_name::<&str>());
                            assert_eq!(err.cql_type, ColumnType::Native(NativeType::Blob));
                            assert_matches!(
                                err.kind,
                                BuiltinTypeCheckErrorKind::MismatchedType {
                                    expected: &[
                                        ColumnType::Native(NativeType::Ascii),
                                        ColumnType::Native(NativeType::Text)
                                    ]
                                }
                            );
                        }
                    }

                    // Deserialization errors
                    {
                        // Got null
                        {
                            let typ = udt_def_with_fields([
                                ("c", ColumnType::Native(NativeType::Boolean)),
                                ("a", ColumnType::Native(NativeType::Blob)),
                                ("b", ColumnType::Native(NativeType::Int)),
                            ]);

                            let err = Udt::deserialize(&typ, None).unwrap_err();
                            let err = get_deser_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                            assert_eq!(err.cql_type, typ);
                            assert_matches!(
                                err.kind,
                                BuiltinDeserializationErrorKind::ExpectedNonNull
                            );
                        }

                        // UDT field deserialization failed
                        {
                            let typ = udt_def_with_fields([
                                ("a", ColumnType::Native(NativeType::Ascii)),
                                ("c", ColumnType::Native(NativeType::Boolean)),
                            ]);

                            let udt_bytes = UdtSerializer::new()
                                .field("alamakota".as_bytes())
                                .field(&42_i16.to_be_bytes())
                                .finalize();

                            let err = deserialize::<Udt>(&typ, &udt_bytes).unwrap_err();

                            let err = get_deser_err(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                            assert_eq!(err.cql_type, typ);
                            let BuiltinDeserializationErrorKind::UdtError(
                                UdtDeserializationErrorKind::FieldDeserializationFailed {
                                    ref field_name,
                                    ref err,
                                },
                            ) = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(field_name.as_str(), "c");
                            let err = get_deser_err_inner(err);
                            assert_eq!(err.rust_name, std::any::type_name::<bool>());
                            assert_eq!(err.cql_type, ColumnType::Native(NativeType::Boolean));
                            assert_matches!(
                                err.kind,
                                BuiltinDeserializationErrorKind::ByteLengthMismatch {
                                    expected: 1,
                                    got: 2,
                                }
                            );
                        }
                    }
                }

                // Strict ordering
                {
                    #[derive(scylla_macros::DeserializeValue, PartialEq, Eq, Debug)]
                    #[scylla(crate = "crate", flavor = "enforce_order", forbid_excess_udt_fields)]
                    struct Udt<'a> {
                        a: &'a str,
                        #[scylla(skip)]
                        x: String,
                        b: Option<i32>,
                        #[scylla(allow_missing)]
                        c: bool,
                    }

                    // Type check errors
                    {
                        // Not UDT
                        {
                            let typ = ColumnType::Collection {
                                frozen: false,
                                typ: CollectionType::Map(
                                    Box::new(ColumnType::Native(NativeType::Ascii)),
                                    Box::new(ColumnType::Native(NativeType::Blob)),
                                ),
                            };
                            let err = Udt::type_check(&typ).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                            assert_eq!(err.cql_type, typ);
                            let BuiltinTypeCheckErrorKind::UdtError(UdtTypeCheckErrorKind::NotUdt) =
                                err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                        }

                        // UDT too few fields
                        {
                            let typ =
                                udt_def_with_fields([("a", ColumnType::Native(NativeType::Text))]);
                            let err = Udt::type_check(&typ).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                            assert_eq!(err.cql_type, typ);
                            let BuiltinTypeCheckErrorKind::UdtError(
                                UdtTypeCheckErrorKind::TooFewFields {
                                    ref required_fields,
                                    ref present_fields,
                                },
                            ) = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(required_fields.as_slice(), &["a", "b"]);
                            assert_eq!(present_fields.as_slice(), &["a".to_string()]);
                        }

                        // excess fields in UDT
                        {
                            let typ = udt_def_with_fields([
                                ("a", ColumnType::Native(NativeType::Text)),
                                ("b", ColumnType::Native(NativeType::Int)),
                                ("d", ColumnType::Native(NativeType::Boolean)),
                            ]);
                            let err = Udt::type_check(&typ).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                            assert_eq!(err.cql_type, typ);
                            let BuiltinTypeCheckErrorKind::UdtError(
                                UdtTypeCheckErrorKind::ExcessFieldInUdt { ref db_field_name },
                            ) = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(db_field_name.as_str(), "d");
                        }

                        // UDT fields switched - field name mismatch
                        {
                            let typ = udt_def_with_fields([
                                ("b", ColumnType::Native(NativeType::Int)),
                                ("a", ColumnType::Native(NativeType::Text)),
                            ]);
                            let err = Udt::type_check(&typ).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                            assert_eq!(err.cql_type, typ);
                            let BuiltinTypeCheckErrorKind::UdtError(
                                UdtTypeCheckErrorKind::FieldNameMismatch {
                                    position,
                                    ref rust_field_name,
                                    ref db_field_name,
                                },
                            ) = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(position, 0);
                            assert_eq!(rust_field_name.as_str(), "a".to_owned());
                            assert_eq!(db_field_name.as_str(), "b".to_owned());
                        }

                        // UDT fields incompatible types - field type check failed
                        {
                            let typ = udt_def_with_fields([
                                ("a", ColumnType::Native(NativeType::Blob)),
                                ("b", ColumnType::Native(NativeType::Int)),
                            ]);
                            let err = Udt::type_check(&typ).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                            assert_eq!(err.cql_type, typ);
                            let BuiltinTypeCheckErrorKind::UdtError(
                                UdtTypeCheckErrorKind::FieldTypeCheckFailed {
                                    ref field_name,
                                    ref err,
                                },
                            ) = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(field_name.as_str(), "a");
                            let err = get_typeck_err_inner(err);
                            assert_eq!(err.rust_name, std::any::type_name::<&str>());
                            assert_eq!(err.cql_type, ColumnType::Native(NativeType::Blob));
                            assert_matches!(
                                err.kind,
                                BuiltinTypeCheckErrorKind::MismatchedType {
                                    expected: &[
                                        ColumnType::Native(NativeType::Ascii),
                                        ColumnType::Native(NativeType::Text)
                                    ]
                                }
                            );
                        }
                    }

                    // Deserialization errors
                    {
                        // Got null
                        {
                            let typ = udt_def_with_fields([
                                ("a", ColumnType::Native(NativeType::Text)),
                                ("b", ColumnType::Native(NativeType::Int)),
                                ("c", ColumnType::Native(NativeType::Boolean)),
                            ]);

                            let err = Udt::deserialize(&typ, None).unwrap_err();
                            let err = get_deser_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                            assert_eq!(err.cql_type, typ);
                            assert_matches!(
                                err.kind,
                                BuiltinDeserializationErrorKind::ExpectedNonNull
                            );
                        }

                        // Bad field format
                        {
                            let typ = udt_def_with_fields([
                                ("a", ColumnType::Native(NativeType::Text)),
                                ("b", ColumnType::Native(NativeType::Int)),
                                ("c", ColumnType::Native(NativeType::Boolean)),
                            ]);

                            let udt_bytes = UdtSerializer::new()
                                .field(b"alamakota")
                                .field(&42_i64.to_be_bytes())
                                .field(&[true as u8])
                                .finalize();

                            let udt_bytes_too_short = udt_bytes.slice(..udt_bytes.len() - 1);
                            assert!(udt_bytes.len() > udt_bytes_too_short.len());

                            let err = deserialize::<Udt>(&typ, &udt_bytes_too_short).unwrap_err();

                            let err = get_deser_err(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                            assert_eq!(err.cql_type, typ);
                            let BuiltinDeserializationErrorKind::RawCqlBytesReadError(_) = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                        }

                        // UDT field deserialization failed
                        {
                            let typ = udt_def_with_fields([
                                ("a", ColumnType::Native(NativeType::Text)),
                                ("b", ColumnType::Native(NativeType::Int)),
                                ("c", ColumnType::Native(NativeType::Boolean)),
                            ]);

                            let udt_bytes = UdtSerializer::new()
                                .field(b"alamakota")
                                .field(&42_i64.to_be_bytes())
                                .field(&[true as u8])
                                .finalize();

                            let err = deserialize::<Udt>(&typ, &udt_bytes).unwrap_err();

                            let err = get_deser_err(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<Udt>());
                            assert_eq!(err.cql_type, typ);
                            let BuiltinDeserializationErrorKind::UdtError(
                                UdtDeserializationErrorKind::FieldDeserializationFailed {
                                    ref field_name,
                                    ref err,
                                },
                            ) = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(field_name.as_str(), "b");
                            let err = get_deser_err_inner(err);
                            assert_eq!(err.rust_name, std::any::type_name::<Option<i32>>());
                            assert_eq!(err.cql_type, ColumnType::Native(NativeType::Int));
                            assert_matches!(
                                err.kind,
                                BuiltinDeserializationErrorKind::ByteLengthMismatch {
                                    expected: 4,
                                    got: 8,
                                }
                            );
                        }
                    }
                }
            }

            #[test]
            fn metadata_does_not_bound_deserialized_values() {
                /* This test covers the UDT part of the lifetime check.
                 * The non-macro parts (blob, str, list, set, map) are tested in value_tests.rs.
                 *
                 * A _deserialized value_ should not be bound by 'metadata - only by 'frame.
                 * This test's goal is only to compile, asserting that lifetimes are correct.
                 */

                // We don't care about the actual deserialized data - all `Err`s is OK.
                // This test's goal is only to compile, asserting that lifetimes are correct.
                let bytes = Bytes::new();

                // By this binding, we require that the deserialized values live longer than metadata.
                let _decoded_udt_res = {
                    // Metadata's lifetime is limited to this scope.

                    // UDT
                    let udt_typ = ColumnType::UserDefinedType {
                        frozen: false,
                        definition: Arc::new(UserDefinedType {
                            name: "udt".into(),
                            keyspace: "ks".into(),
                            field_types: vec![
                                ("bytes".into(), ColumnType::Native(NativeType::Blob)),
                                ("text".into(), ColumnType::Native(NativeType::Text)),
                            ],
                        }),
                    };
                    #[derive(DeserializeValue)]
                    #[scylla(crate = crate)]
                    struct Udt<'frame> {
                        #[expect(dead_code)]
                        bytes: &'frame [u8],
                        #[expect(dead_code)]
                        text: &'frame str,
                    }
                    deserialize::<Udt>(&udt_typ, &bytes)
                };
            }
        }
    }

    mod row {
        use bytes::Bytes;

        use crate::deserialize::row::{ColumnIterator, DeserializeRow};
        use crate::deserialize::tests::spec;
        use crate::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
        use crate::frame::response::result::ColumnSpec;
        use crate::frame::response::result::{ColumnType, NativeType};
        use crate::serialize::row::{RowSerializationContext, SerializeRow};
        use crate::serialize::writers::RowWriter;

        #[derive(Debug)]
        enum TestDeserializeError {
            TypeCheck(#[expect(dead_code)] TypeCheckError),
            Deserialization(DeserializationError),
        }

        fn deserialize<'frame, 'metadata, R>(
            specs: &'metadata [ColumnSpec<'metadata>],
            byts: &'frame Bytes,
        ) -> Result<R, TestDeserializeError>
        where
            R: DeserializeRow<'frame, 'metadata>,
        {
            <R as DeserializeRow<'frame, 'metadata>>::type_check(specs)
                .map_err(TestDeserializeError::TypeCheck)?;
            let slice = FrameSlice::new(byts);
            let iter = ColumnIterator::new(specs, slice);
            <R as DeserializeRow<'frame, 'metadata>>::deserialize(iter)
                .map_err(TestDeserializeError::Deserialization)
        }

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

        mod deserialize {
            use assert_matches::assert_matches;
            use bytes::Bytes;

            use super::{TestDeserializeError, deserialize, spec};
            use crate::DeserializeRow;
            use crate::deserialize::row::{
                BuiltinDeserializationError, BuiltinDeserializationErrorKind,
                BuiltinTypeCheckError, BuiltinTypeCheckErrorKind, ColumnIterator, DeserializeRow,
            };
            use crate::deserialize::tests::serialize_cells;
            use crate::deserialize::value;
            use crate::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
            use crate::frame::response::result::{ColumnSpec, TableSpec};
            use crate::frame::response::result::{ColumnType, NativeType};

            fn val_int(i: i32) -> Option<Vec<u8>> {
                Some(i.to_be_bytes().to_vec())
            }

            fn val_str(s: &str) -> Option<Vec<u8>> {
                Some(s.as_bytes().to_vec())
            }

            #[track_caller]
            fn get_typeck_err_inner(err: &TypeCheckError) -> &BuiltinTypeCheckError {
                match err.downcast_ref() {
                    Some(err) => err,
                    None => panic!("not a BuiltinTypeCheckError: {err:?}"),
                }
            }

            #[track_caller]
            fn get_deser_err_inner(err: &DeserializationError) -> &BuiltinDeserializationError {
                match err.downcast_ref() {
                    Some(err) => err,
                    None => panic!("not a BuiltinDeserializationError: {err:?}"),
                }
            }

            #[track_caller]
            fn get_deser_err(err: &TestDeserializeError) -> &BuiltinDeserializationError {
                match err {
                    TestDeserializeError::Deserialization(err) => get_deser_err_inner(err),
                    other => panic!("expected Deserialization error, got: {other:?}"),
                }
            }

            #[track_caller]
            fn get_value_typeck_err_inner(err: &TypeCheckError) -> &value::BuiltinTypeCheckError {
                match err.downcast_ref() {
                    Some(err) => err,
                    None => panic!("not a value::BuiltinTypeCheckError: {err:?}"),
                }
            }

            #[track_caller]
            fn get_value_deser_err_inner(
                err: &DeserializationError,
            ) -> &value::BuiltinDeserializationError {
                match err.downcast_ref() {
                    Some(err) => err,
                    None => panic!("not a value::BuiltinDeserializationError: {err:?}"),
                }
            }

            fn specs_to_types<'a>(specs: &[ColumnSpec<'a>]) -> Vec<ColumnType<'a>> {
                specs.iter().map(|spec| spec.typ().clone()).collect()
            }

            // Do not remove. It's not used in tests but we keep it here to check that
            // we properly ignore warnings about unused variables, unnecessary `mut`s
            // etc. that usually pop up when generating code for empty structs.
            #[derive(DeserializeRow)]
            #[scylla(crate = crate)]
            struct TestUdtWithNoFieldsUnordered {}

            #[derive(DeserializeRow)]
            #[scylla(crate = crate, flavor = "enforce_order")]
            struct TestUdtWithNoFieldsOrdered {}

            // If deserialize is never called, rust warns that the struct is never constructed.
            // We don't want to `expect(dead_code)` on struct definitions, because that could silence
            // some warnings that this test is supposed to prevent.
            #[expect(unreachable_code, dead_code)]
            fn dummy_deserialize_udts() {
                let _ = deserialize::<TestUdtWithNoFieldsUnordered>(todo!(), todo!()).unwrap();
                let _ = deserialize::<TestUdtWithNoFieldsOrdered>(todo!(), todo!()).unwrap();
            }

            #[test]
            fn test_struct_deserialization_loose_ordering() {
                #[derive(DeserializeRow, PartialEq, Eq, Debug)]
                #[scylla(crate = "crate")]
                struct MyRow<'a> {
                    a: &'a str,
                    b: Option<i32>,
                    #[scylla(skip)]
                    c: String,
                    #[scylla(default_when_null)]
                    d: i32,
                    #[scylla(default_when_null)]
                    e: &'a str,
                }

                // Original order of columns
                let specs = &[
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("d", ColumnType::Native(NativeType::Int)),
                    spec("e", ColumnType::Native(NativeType::Text)),
                ];
                let byts = serialize_cells([val_str("abc"), val_int(123), None, val_str("def")]);
                let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
                assert_eq!(
                    row,
                    MyRow {
                        a: "abc",
                        b: Some(123),
                        c: String::new(),
                        d: 0,
                        e: "def",
                    }
                );

                // Different order of columns - should still work
                let specs = &[
                    spec("e", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("d", ColumnType::Native(NativeType::Int)),
                    spec("a", ColumnType::Native(NativeType::Text)),
                ];
                let byts = serialize_cells([None, val_int(123), None, val_str("abc")]);
                let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
                assert_eq!(
                    row,
                    MyRow {
                        a: "abc",
                        b: Some(123),
                        c: String::new(),
                        d: 0,
                        e: "",
                    }
                );

                // Missing column
                let specs = &[
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("e", ColumnType::Native(NativeType::Text)),
                ];
                MyRow::type_check(specs).unwrap_err();

                // Missing both default_when_null column
                let specs = &[
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                ];
                MyRow::type_check(specs).unwrap_err();

                // Wrong column type
                let specs = &[
                    spec("a", ColumnType::Native(NativeType::Int)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                ];
                MyRow::type_check(specs).unwrap_err();
            }

            #[test]
            fn test_struct_deserialization_strict_ordering() {
                #[derive(DeserializeRow, PartialEq, Eq, Debug)]
                #[scylla(crate = "crate", flavor = "enforce_order")]
                struct MyRow<'a> {
                    a: &'a str,
                    b: Option<i32>,
                    #[scylla(skip)]
                    c: String,
                    #[scylla(default_when_null)]
                    d: i32,
                    #[scylla(default_when_null)]
                    e: &'a str,
                }

                // Correct order of columns
                let specs = &[
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("d", ColumnType::Native(NativeType::Int)),
                    spec("e", ColumnType::Native(NativeType::Text)),
                ];
                let byts = serialize_cells([val_str("abc"), val_int(123), None, val_str("def")]);
                let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
                assert_eq!(
                    row,
                    MyRow {
                        a: "abc",
                        b: Some(123),
                        c: String::new(),
                        d: 0,
                        e: "def",
                    }
                );

                // Wrong order of columns
                let specs = &[
                    spec("b", ColumnType::Native(NativeType::Int)),
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("d", ColumnType::Native(NativeType::Int)),
                    spec("e", ColumnType::Native(NativeType::Text)),
                ];
                MyRow::type_check(specs).unwrap_err();

                // Missing column
                let specs = &[
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("e", ColumnType::Native(NativeType::Text)),
                ];
                MyRow::type_check(specs).unwrap_err();

                // Missing both default_when_null column
                let specs = &[
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                ];
                MyRow::type_check(specs).unwrap_err();

                // Wrong column type
                let specs = &[
                    spec("a", ColumnType::Native(NativeType::Int)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                ];
                MyRow::type_check(specs).unwrap_err();
            }

            #[test]
            fn test_struct_deserialization_no_name_check() {
                #[derive(DeserializeRow, PartialEq, Eq, Debug)]
                #[scylla(crate = "crate", flavor = "enforce_order", skip_name_checks)]
                struct MyRow<'a> {
                    a: &'a str,
                    b: Option<i32>,
                    #[scylla(skip)]
                    c: String,
                }

                // Correct order of columns
                let specs = &[
                    spec("a", ColumnType::Native(NativeType::Text)),
                    spec("b", ColumnType::Native(NativeType::Int)),
                ];
                let byts = serialize_cells([val_str("abc"), val_int(123)]);
                let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
                assert_eq!(
                    row,
                    MyRow {
                        a: "abc",
                        b: Some(123),
                        c: String::new(),
                    }
                );

                // Correct order of columns, but different names - should still succeed
                let specs = &[
                    spec("z", ColumnType::Native(NativeType::Text)),
                    spec("x", ColumnType::Native(NativeType::Int)),
                ];
                let byts = serialize_cells([val_str("abc"), val_int(123)]);
                let row = deserialize::<MyRow<'_>>(specs, &byts).unwrap();
                assert_eq!(
                    row,
                    MyRow {
                        a: "abc",
                        b: Some(123),
                        c: String::new(),
                    }
                );
            }

            #[test]
            fn test_struct_deserialization_cross_rename_fields() {
                #[derive(scylla_macros::DeserializeRow, PartialEq, Eq, Debug)]
                #[scylla(crate = crate)]
                struct TestRow {
                    #[scylla(rename = "b")]
                    a: i32,
                    #[scylla(rename = "a")]
                    b: String,
                }

                // Columns switched wrt fields - should still work.
                {
                    let row_bytes = serialize_cells(
                        ["The quick brown fox".as_bytes(), &42_i32.to_be_bytes()].map(Some),
                    );
                    let specs = [
                        spec("a", ColumnType::Native(NativeType::Text)),
                        spec("b", ColumnType::Native(NativeType::Int)),
                    ];

                    let row = deserialize::<TestRow>(&specs, &row_bytes).unwrap();
                    assert_eq!(
                        row,
                        TestRow {
                            a: 42,
                            b: "The quick brown fox".to_owned(),
                        }
                    );
                }
            }

            #[test]
            fn test_struct_deserialization_errors() {
                // Loose ordering
                {
                    #[derive(scylla_macros::DeserializeRow, PartialEq, Eq, Debug)]
                    #[scylla(crate = "crate")]
                    struct MyRow<'a> {
                        a: &'a str,
                        #[scylla(skip)]
                        x: String,
                        b: Option<i32>,
                        #[scylla(rename = "c")]
                        d: bool,
                        #[scylla(default_when_null)]
                        e: Option<i32>,
                    }

                    // Type check errors
                    {
                        // Missing column
                        {
                            let specs = [
                                spec("a", ColumnType::Native(NativeType::Ascii)),
                                spec("b", ColumnType::Native(NativeType::Int)),
                            ];
                            let err = MyRow::type_check(&specs).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                            assert_eq!(err.cql_types, specs_to_types(&specs));
                            let BuiltinTypeCheckErrorKind::ValuesMissingForColumns {
                                column_names: ref missing_fields,
                            } = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(missing_fields.as_slice(), &["c", "e"]);
                        }

                        // Duplicated column
                        {
                            let specs = [
                                spec("a", ColumnType::Native(NativeType::Ascii)),
                                spec("b", ColumnType::Native(NativeType::Int)),
                                spec("a", ColumnType::Native(NativeType::Ascii)),
                            ];

                            let err = MyRow::type_check(&specs).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                            assert_eq!(err.cql_types, specs_to_types(&specs));
                            let BuiltinTypeCheckErrorKind::DuplicatedColumn {
                                column_index,
                                column_name,
                            } = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(column_index, 2);
                            assert_eq!(column_name, "a");
                        }

                        // Unknown column
                        {
                            let specs = [
                                spec("d", ColumnType::Native(NativeType::Counter)),
                                spec("a", ColumnType::Native(NativeType::Ascii)),
                                spec("b", ColumnType::Native(NativeType::Int)),
                            ];

                            let err = MyRow::type_check(&specs).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                            assert_eq!(err.cql_types, specs_to_types(&specs));
                            let BuiltinTypeCheckErrorKind::ColumnWithUnknownName {
                                column_index,
                                ref column_name,
                            } = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(column_index, 0);
                            assert_eq!(column_name.as_str(), "d");
                        }

                        // Column incompatible types - column type check failed
                        {
                            let specs = [
                                spec("b", ColumnType::Native(NativeType::Int)),
                                spec("a", ColumnType::Native(NativeType::Blob)),
                            ];
                            let err = MyRow::type_check(&specs).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                            assert_eq!(err.cql_types, specs_to_types(&specs));
                            let BuiltinTypeCheckErrorKind::ColumnTypeCheckFailed {
                                column_index,
                                ref column_name,
                                ref err,
                            } = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(column_index, 1);
                            assert_eq!(column_name.as_str(), "a");
                            let err = get_value_typeck_err_inner(err);
                            assert_eq!(err.rust_name, std::any::type_name::<&str>());
                            assert_eq!(err.cql_type, ColumnType::Native(NativeType::Blob));
                            assert_matches!(
                                err.kind,
                                value::BuiltinTypeCheckErrorKind::MismatchedType {
                                    expected: &[
                                        ColumnType::Native(NativeType::Ascii),
                                        ColumnType::Native(NativeType::Text)
                                    ]
                                }
                            );
                        }
                    }

                    // Deserialization errors
                    {
                        // Got null
                        {
                            let specs = [
                                spec("c", ColumnType::Native(NativeType::Boolean)),
                                spec("a", ColumnType::Native(NativeType::Blob)),
                                spec("b", ColumnType::Native(NativeType::Int)),
                                spec("e", ColumnType::Native(NativeType::Int)),
                            ];

                            let err = MyRow::deserialize(ColumnIterator::new(
                                &specs,
                                FrameSlice::new(&serialize_cells([Some([true as u8])])),
                            ))
                            .unwrap_err();
                            let err = get_deser_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                            let BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
                                column_index,
                                ref column_name,
                                ..
                            } = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(column_index, 1);
                            assert_eq!(column_name, "a");
                        }

                        // Column deserialization failed
                        {
                            let specs = [
                                spec("b", ColumnType::Native(NativeType::Int)),
                                spec("a", ColumnType::Native(NativeType::Ascii)),
                                spec("c", ColumnType::Native(NativeType::Boolean)),
                                spec("e", ColumnType::Native(NativeType::Int)),
                            ];

                            let row_bytes = serialize_cells(
                                [
                                    &0_i32.to_be_bytes(),
                                    "alamakota".as_bytes(),
                                    &42_i16.to_be_bytes(),
                                    &13_i32.to_be_bytes(),
                                ]
                                .map(Some),
                            );

                            let err = deserialize::<MyRow>(&specs, &row_bytes).unwrap_err();

                            let err = get_deser_err(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                            let BuiltinDeserializationErrorKind::ColumnDeserializationFailed {
                                column_index,
                                ref column_name,
                                ref err,
                            } = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(column_index, 2);
                            assert_eq!(column_name.as_str(), "c");
                            let err = get_value_deser_err_inner(err);
                            assert_eq!(err.rust_name, std::any::type_name::<bool>());
                            assert_eq!(err.cql_type, ColumnType::Native(NativeType::Boolean));
                            assert_matches!(
                                err.kind,
                                value::BuiltinDeserializationErrorKind::ByteLengthMismatch {
                                    expected: 1,
                                    got: 2,
                                }
                            );
                        }
                    }
                }

                // Strict ordering
                {
                    #[derive(scylla_macros::DeserializeRow, PartialEq, Eq, Debug)]
                    #[scylla(crate = "crate", flavor = "enforce_order")]
                    struct MyRow<'a> {
                        a: &'a str,
                        #[scylla(skip)]
                        x: String,
                        b: Option<i32>,
                        c: bool,
                        #[scylla(default_when_null)]
                        d: Option<i32>,
                    }

                    // Type check errors
                    {
                        // Too few columns
                        {
                            let specs = [spec("a", ColumnType::Native(NativeType::Text))];
                            let err = MyRow::type_check(&specs).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                            assert_eq!(err.cql_types, specs_to_types(&specs));
                            let BuiltinTypeCheckErrorKind::WrongColumnCount {
                                rust_cols,
                                cql_cols,
                            } = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(rust_cols, 4);
                            assert_eq!(cql_cols, 1);
                        }

                        // Excess columns
                        {
                            let specs = [
                                spec("a", ColumnType::Native(NativeType::Text)),
                                spec("b", ColumnType::Native(NativeType::Int)),
                                spec("c", ColumnType::Native(NativeType::Boolean)),
                                spec("d", ColumnType::Native(NativeType::Int)),
                                spec("e", ColumnType::Native(NativeType::Counter)),
                            ];
                            let err = MyRow::type_check(&specs).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                            assert_eq!(err.cql_types, specs_to_types(&specs));
                            let BuiltinTypeCheckErrorKind::WrongColumnCount {
                                rust_cols,
                                cql_cols,
                            } = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(rust_cols, 4);
                            assert_eq!(cql_cols, 5);
                        }

                        // Renamed column name mismatch
                        {
                            let specs = [
                                spec("a", ColumnType::Native(NativeType::Text)),
                                spec("b", ColumnType::Native(NativeType::Int)),
                                spec("e", ColumnType::Native(NativeType::Boolean)),
                                spec("d", ColumnType::Native(NativeType::Int)),
                            ];
                            let err = MyRow::type_check(&specs).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                            let BuiltinTypeCheckErrorKind::ColumnNameMismatch {
                                field_index,
                                column_index,
                                rust_column_name,
                                ref db_column_name,
                            } = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(field_index, 3);
                            assert_eq!(rust_column_name, "c");
                            assert_eq!(column_index, 2);
                            assert_eq!(db_column_name.as_str(), "e");
                        }

                        // Columns switched - column name mismatch
                        {
                            let specs = [
                                spec("b", ColumnType::Native(NativeType::Int)),
                                spec("a", ColumnType::Native(NativeType::Text)),
                                spec("c", ColumnType::Native(NativeType::Boolean)),
                                spec("d", ColumnType::Native(NativeType::Int)),
                            ];
                            let err = MyRow::type_check(&specs).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                            assert_eq!(err.cql_types, specs_to_types(&specs));
                            let BuiltinTypeCheckErrorKind::ColumnNameMismatch {
                                field_index,
                                column_index,
                                rust_column_name,
                                ref db_column_name,
                            } = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(field_index, 0);
                            assert_eq!(column_index, 0);
                            assert_eq!(rust_column_name, "a");
                            assert_eq!(db_column_name.as_str(), "b");
                        }

                        // Column incompatible types - column type check failed
                        {
                            let specs = [
                                spec("a", ColumnType::Native(NativeType::Blob)),
                                spec("b", ColumnType::Native(NativeType::Int)),
                                spec("c", ColumnType::Native(NativeType::Boolean)),
                                spec("d", ColumnType::Native(NativeType::Int)),
                            ];
                            let err = MyRow::type_check(&specs).unwrap_err();
                            let err = get_typeck_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                            assert_eq!(err.cql_types, specs_to_types(&specs));
                            let BuiltinTypeCheckErrorKind::ColumnTypeCheckFailed {
                                column_index,
                                ref column_name,
                                ref err,
                            } = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(column_index, 0);
                            assert_eq!(column_name.as_str(), "a");
                            let err = get_value_typeck_err_inner(err);
                            assert_eq!(err.rust_name, std::any::type_name::<&str>());
                            assert_eq!(err.cql_type, ColumnType::Native(NativeType::Blob));
                            assert_matches!(
                                err.kind,
                                value::BuiltinTypeCheckErrorKind::MismatchedType {
                                    expected: &[
                                        ColumnType::Native(NativeType::Ascii),
                                        ColumnType::Native(NativeType::Text)
                                    ]
                                }
                            );
                        }
                    }

                    // Deserialization errors
                    {
                        // Too few columns
                        {
                            let specs = [
                                spec("a", ColumnType::Native(NativeType::Text)),
                                spec("b", ColumnType::Native(NativeType::Int)),
                                spec("c", ColumnType::Native(NativeType::Boolean)),
                                spec("d", ColumnType::Native(NativeType::Int)),
                            ];

                            let err = MyRow::deserialize(ColumnIterator::new(
                                &specs,
                                FrameSlice::new(&serialize_cells([Some([true as u8])])),
                            ))
                            .unwrap_err();
                            let err = get_deser_err_inner(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                            let BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
                                column_index,
                                ref column_name,
                                ..
                            } = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(column_index, 1);
                            assert_eq!(column_name, "b");
                        }

                        // Bad field format
                        {
                            let typ = [
                                spec("a", ColumnType::Native(NativeType::Text)),
                                spec("b", ColumnType::Native(NativeType::Int)),
                                spec("c", ColumnType::Native(NativeType::Boolean)),
                                spec("d", ColumnType::Native(NativeType::Int)),
                            ];

                            let row_bytes = serialize_cells(
                                [
                                    (&b"alamakota"[..]),
                                    &42_i32.to_be_bytes(),
                                    &[true as u8],
                                    &13_i32.to_be_bytes(),
                                ]
                                .map(Some),
                            );

                            let row_bytes_too_short = row_bytes.slice(..row_bytes.len() - 1);
                            assert!(row_bytes.len() > row_bytes_too_short.len());

                            let err = deserialize::<MyRow>(&typ, &row_bytes_too_short).unwrap_err();

                            let err = get_deser_err(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                            let BuiltinDeserializationErrorKind::RawColumnDeserializationFailed {
                                column_index,
                                ref column_name,
                                ..
                            } = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(column_index, 3);
                            assert_eq!(column_name, "d");
                        }

                        // Column deserialization failed
                        {
                            let specs = [
                                spec("a", ColumnType::Native(NativeType::Text)),
                                spec("b", ColumnType::Native(NativeType::Int)),
                                spec("c", ColumnType::Native(NativeType::Boolean)),
                                spec("d", ColumnType::Native(NativeType::Int)),
                            ];

                            let row_bytes = serialize_cells(
                                [
                                    &b"alamakota"[..],
                                    &42_i64.to_be_bytes(),
                                    &[true as u8],
                                    &13_i32.to_be_bytes(),
                                ]
                                .map(Some),
                            );

                            let err = deserialize::<MyRow>(&specs, &row_bytes).unwrap_err();

                            let err = get_deser_err(&err);
                            assert_eq!(err.rust_name, std::any::type_name::<MyRow>());
                            let BuiltinDeserializationErrorKind::ColumnDeserializationFailed {
                                column_index: field_index,
                                ref column_name,
                                ref err,
                            } = err.kind
                            else {
                                panic!("unexpected error kind: {:?}", err.kind)
                            };
                            assert_eq!(column_name.as_str(), "b");
                            assert_eq!(field_index, 2);
                            let err = get_value_deser_err_inner(err);
                            assert_eq!(err.rust_name, std::any::type_name::<Option<i32>>());
                            assert_eq!(err.cql_type, ColumnType::Native(NativeType::Int));
                            assert_matches!(
                                err.kind,
                                value::BuiltinDeserializationErrorKind::ByteLengthMismatch {
                                    expected: 4,
                                    got: 8,
                                }
                            );
                        }
                    }
                }
            }

            #[test]
            fn metadata_does_not_bound_deserialized_rows() {
                /* It's important to understand what is a _deserialized row_. It's not just
                 * an implementor of `DeserializeRow`; there are some implementors of `DeserializeRow`
                 * who are not yet final rows, but partially deserialized rows that support further
                 * deserialization - _row deserializers_, such as `ColumnIterator`.
                 * _Row deserializers_, because they still need to deserialize some row, are naturally
                 * bound by 'metadata lifetime. However, _rows_ are completely deserialized, so they
                 * should not be bound by 'metadata - only by 'frame. This test asserts that.
                 */

                // We don't care about the actual deserialized data - all `Err`s is OK.
                // This test's goal is only to compile, asserting that lifetimes are correct.
                let bytes = Bytes::new();

                // By this binding, we require that the deserialized rows live longer than metadata.
                let _decoded_results = {
                    // Metadata's lifetime is limited to this scope.

                    fn col_spec<'a>(name: &'a str, typ: ColumnType<'a>) -> ColumnSpec<'a> {
                        ColumnSpec::borrowed(name, typ, TableSpec::borrowed("ks", "tbl"))
                    }

                    let row_typ = &[
                        col_spec("bytes", ColumnType::Native(NativeType::Blob)),
                        col_spec("text", ColumnType::Native(NativeType::Text)),
                    ];

                    #[derive(DeserializeRow)]
                    #[scylla(crate=crate)]
                    struct MyRow<'frame> {
                        #[expect(dead_code)]
                        bytes: &'frame [u8],
                        #[expect(dead_code)]
                        text: &'frame str,
                    }

                    deserialize::<MyRow>(row_typ, &bytes)
                };
            }
        }
    }
}
