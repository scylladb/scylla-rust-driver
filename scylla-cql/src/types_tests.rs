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

mod enum_serialization_tests {
    use crate::deserialize::FrameSlice;
    use crate::deserialize::value::DeserializeValue;
    use crate::frame::response::result::{ColumnType, NativeType};
    use crate::serialize::value::SerializeValue;
    use crate::serialize::writers::CellWriter;
    use bytes::Bytes;
    use scylla_macros::{DeserializeValue, SerializeValue};

    // --- TEST HELPERS ---

    fn assert_round_trip<T>(value: T, typ: ColumnType, expected_bytes: &[u8])
    where
        T: SerializeValue
            + for<'f, 'm> DeserializeValue<'f, 'm>
            + PartialEq
            + std::fmt::Debug
            + Copy,
    {
        // Serialization Test
        let mut data = Vec::new();
        let writer = CellWriter::new(&mut data);
        value.serialize(&typ, writer).unwrap();

        // Verify length header (4 bytes big endian) + data
        let len = expected_bytes.len() as i32;
        let mut expected_full = len.to_be_bytes().to_vec();
        expected_full.extend_from_slice(expected_bytes);

        assert_eq!(data, expected_full, "Serialization failed (byte mismatch)");

        // Deserialization Test
        // Simulate reading from a frame (skip 4 bytes length header)
        let payload = Bytes::copy_from_slice(expected_bytes);
        let slice = FrameSlice::new(&payload);

        let deserialized = T::deserialize(&typ, Some(slice)).expect("Deserialization failed");
        assert_eq!(
            value, deserialized,
            "Deserialized value does not match input"
        );
    }

    fn assert_deser_error<T>(typ: ColumnType, bad_bytes: &[u8])
    where
        T: for<'f, 'm> DeserializeValue<'f, 'm> + std::fmt::Debug,
    {
        let payload = Bytes::copy_from_slice(bad_bytes);
        let slice = FrameSlice::new(&payload);
        let result = T::deserialize(&typ, Some(slice));
        assert!(
            result.is_err(),
            "Expected error, but deserialization succeeded: {:?}",
            result
        );
    }

    // --- ENUM DEFINITIONS ---

    #[derive(SerializeValue, DeserializeValue, Debug, PartialEq, Copy, Clone)]
    #[scylla(crate = "crate")]
    #[repr(i32)]
    enum DefaultInt {
        A = 0,
        B = 1,
        C = 100,
    }

    #[derive(SerializeValue, DeserializeValue, Debug, PartialEq, Copy, Clone)]
    #[scylla(crate = "crate")]
    #[repr(i8)]
    enum TinyEnum {
        Small = 10,
        Max = 127,
    }

    #[derive(SerializeValue, DeserializeValue, Debug, PartialEq, Copy, Clone)]
    #[scylla(crate = "crate")]
    #[repr(i16)]
    enum SmallEnum {
        Kilo = 1000,
        BigKilo = 30000,
    }

    #[derive(SerializeValue, DeserializeValue, Debug, PartialEq, Copy, Clone)]
    #[repr(i64)]
    #[scylla(crate = "crate")]
    enum BigEnum {
        Big = 5_000_000_000,
    }

    #[derive(SerializeValue, DeserializeValue, Debug, PartialEq, Copy, Clone)]
    #[scylla(crate = "crate")]
    #[repr(i32)]
    enum Gaps {
        First = 1,
        Jump = 5,
        FarAway = 99,
    }

    #[derive(SerializeValue, DeserializeValue, Debug, PartialEq, Copy, Clone)]
    #[scylla(crate = "crate")]
    #[repr(i32)]
    enum NegativeEnum {
        MinusOne = -1,
        MinusHundred = -100,
    }

    #[derive(SerializeValue, DeserializeValue, Debug, PartialEq, Copy, Clone)]
    #[scylla(crate = "crate")]
    #[repr(i32)]
    enum ImplicitEnum {
        First,
        Second,
        Third,
    }

    // --- TESTS ---

    #[test]
    fn test_i32_standard() {
        let typ = ColumnType::Native(NativeType::Int);
        assert_round_trip(DefaultInt::A, typ.clone(), &[0, 0, 0, 0]);
        assert_round_trip(DefaultInt::C, typ.clone(), &[0, 0, 0, 100]);
    }

    #[test]
    fn test_i8_tinyint() {
        let typ = ColumnType::Native(NativeType::TinyInt);
        assert_round_trip(TinyEnum::Small, typ.clone(), &[10]);
        assert_round_trip(TinyEnum::Max, typ.clone(), &[0x7F]);
    }

    #[test]
    fn test_i16_smallint() {
        let typ = ColumnType::Native(NativeType::SmallInt);
        assert_round_trip(SmallEnum::Kilo, typ.clone(), &[0x03, 0xE8]);
        assert_round_trip(SmallEnum::BigKilo, typ.clone(), &30000i16.to_be_bytes());
    }

    #[test]
    fn test_i64_bigint() {
        let typ = ColumnType::Native(NativeType::BigInt);
        assert_round_trip(BigEnum::Big, typ.clone(), &5_000_000_000i64.to_be_bytes());
    }

    #[test]
    fn test_gaps_in_discriminants() {
        let typ = ColumnType::Native(NativeType::Int);
        assert_round_trip(Gaps::First, typ.clone(), &1i32.to_be_bytes());
        assert_round_trip(Gaps::Jump, typ.clone(), &5i32.to_be_bytes());
        assert_round_trip(Gaps::FarAway, typ.clone(), &99i32.to_be_bytes());

        let bad_val = 2i32.to_be_bytes();
        assert_deser_error::<Gaps>(typ.clone(), &bad_val);
    }

    #[test]
    fn test_negative_discriminants() {
        let typ = ColumnType::Native(NativeType::Int);
        assert_round_trip(NegativeEnum::MinusOne, typ.clone(), &(-1i32).to_be_bytes());
        assert_round_trip(
            NegativeEnum::MinusHundred,
            typ.clone(),
            &(-100i32).to_be_bytes(),
        );
    }

    #[test]
    fn test_implicit_discriminants() {
        let typ = ColumnType::Native(NativeType::Int);
        assert_round_trip(ImplicitEnum::First, typ.clone(), &0i32.to_be_bytes());
        assert_round_trip(ImplicitEnum::Second, typ.clone(), &1i32.to_be_bytes());
        assert_round_trip(ImplicitEnum::Third, typ.clone(), &2i32.to_be_bytes());
    }

    #[test]
    fn test_invalid_data_errors() {
        let typ = ColumnType::Native(NativeType::Int);
        let out_of_range = 999i32.to_be_bytes();
        assert_deser_error::<DefaultInt>(typ.clone(), &out_of_range);

        let too_short = vec![0, 0, 1];
        assert_deser_error::<DefaultInt>(typ.clone(), &too_short);
    }

    #[test]
    fn test_null_handling() {
        let typ = ColumnType::Native(NativeType::Int);
        let result = <DefaultInt as DeserializeValue>::deserialize(&typ, None);
        assert!(
            result.is_err(),
            "Deserializing NULL into non-Option enum should fail"
        );
    }

    #[test]
    fn test_type_check_mismatch() {
        let bad_typ = ColumnType::Native(NativeType::Text);
        let check = <DefaultInt as DeserializeValue>::type_check(&bad_typ);
        assert!(
            check.is_err(),
            "Should return TypeCheck error (Int vs Text)"
        );
    }
}
