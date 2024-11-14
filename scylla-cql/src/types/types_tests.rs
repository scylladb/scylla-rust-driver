mod derive_macros_integration {
    mod value {
        use bytes::Bytes;

        use crate::frame::response::result::ColumnType;
        use crate::types::deserialize::value::tests::{deserialize, udt_def_with_fields};
        use crate::types::serialize::value::tests::do_serialize;

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
                        ("a", ColumnType::Text),
                        ("b", ColumnType::Int),
                        ("c", ColumnType::BigInt),
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
                    udt_def_with_fields([("a", ColumnType::Text), ("c", ColumnType::BigInt)]),
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
                        ("b", ColumnType::Int),
                        ("a", ColumnType::Text),
                        ("c", ColumnType::BigInt),
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
                    udt_def_with_fields([("a", ColumnType::Text), ("b", ColumnType::Int)]),
                    Udt {
                        x: String::new(),
                        ..original_udt
                    },
                ),
                //
                // An excess field at the end of UDT
                (
                    udt_def_with_fields([
                        ("a", ColumnType::Text),
                        ("b", ColumnType::Int),
                        ("d", ColumnType::Boolean),
                    ]),
                    Udt {
                        x: String::new(),
                        ..original_udt
                    },
                ),
                //
                // Missing non-required fields
                (
                    udt_def_with_fields([("a", ColumnType::Text)]),
                    Udt {
                        x: String::new(),
                        b: None,
                        ..original_udt
                    },
                ),
                //
                // An excess field at the end of UDT instead of non-required fields
                (
                    udt_def_with_fields([("d", ColumnType::Boolean)]),
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
    }

    mod row {
        use bytes::Bytes;

        use crate::frame::response::result::ColumnType;
        use crate::types::deserialize::row::tests::deserialize;
        use crate::types::deserialize::tests::spec;
        use crate::types::serialize::row::tests::do_serialize;

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
                        spec("a", ColumnType::Text),
                        spec("b", ColumnType::Int),
                        spec("c", ColumnType::BigInt),
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
                        spec("b", ColumnType::Int),
                        spec("a", ColumnType::Text),
                        spec("c", ColumnType::BigInt),
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
                    &[spec("a", ColumnType::Text), spec("b", ColumnType::Int)][..],
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
