#![no_implicit_prelude]

// Macro that is given a crate name and tests it for hygiene
macro_rules! test_crate {
    ($name:ident) => {
        extern crate $name as _scylla;

        #[derive(
            _scylla::macros::FromRow,
            _scylla::macros::FromUserType,
            _scylla::macros::IntoUserType,
            _scylla::macros::ValueList,
            PartialEq,
            Debug,
        )]
        #[scylla_crate = "_scylla"]
        struct TestStruct {
            a: ::core::primitive::i32,
        }
        #[test]
        fn test_rename() {
            use _scylla::cql_to_rust::{FromCqlVal, FromRow};
            use _scylla::frame::response::result::CqlValue;
            use _scylla::frame::value::{Value, ValueList};

            fn derived<T>()
            where
                T: FromRow + FromCqlVal<CqlValue> + Value + ValueList,
            {
            }
            derived::<TestStruct>();
        }
        #[test]
        fn test_derives() {
            use ::core::primitive::u8;
            use ::std::assert_eq;
            use ::std::option::Option::Some;
            use ::std::vec::Vec;
            use _scylla::_macro_internal::{CqlValue, Row, Value, ValueList};
            use _scylla::cql_to_rust::FromRow;

            let test_struct = TestStruct { a: 16 };
            fn get_row() -> Row {
                Row {
                    columns: ::std::vec![Some(CqlValue::Int(16))],
                }
            }

            let st: TestStruct = FromRow::from_row(get_row()).unwrap();
            assert_eq!(st, test_struct);

            let udt = get_row().into_typed::<TestStruct>().unwrap();
            assert_eq!(udt, test_struct);

            let mut buf = Vec::<u8>::new();
            test_struct.serialize(&mut buf).unwrap();
            let mut buf_assert = Vec::<u8>::new();
            let tuple_with_same_layout: (i32,) = (16,);
            tuple_with_same_layout.serialize(&mut buf_assert).unwrap();
            assert_eq!(buf, buf_assert);

            let sv = test_struct.serialized().unwrap().into_owned();
            let sv2 = tuple_with_same_layout.serialized().unwrap().into_owned();
            assert_eq!(sv, sv2);
        }

        #[derive(_scylla::macros::SerializeCql, _scylla::macros::SerializeRow)]
        #[scylla(crate = _scylla)]
        struct TestStructNew {
            x: ::core::primitive::i32,
        }
    };
}

mod scylla_hygiene {
    test_crate!(scylla);
}
mod scylla_cql_hygiene {
    test_crate!(scylla_cql);
}
