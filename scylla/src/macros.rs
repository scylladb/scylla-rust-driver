/// Prepares query values to be used with queries and prepared statements
#[macro_export]
macro_rules! values {
    ($($value:expr),*) => {
        {
            use $crate::frame::value::Value;
            use ::std::vec::Vec;
            let mut values: Vec<Value> = Vec::new();
            $(
                values.push(::std::convert::Into::into($value));
            )*
            values
        }
    };
}
