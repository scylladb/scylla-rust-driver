mod row {
    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::SerializeRow)]
    /// #[scylla(crate = scylla_cql, skip_name_checks)]
    /// struct TestRow {}
    /// ```
    fn _test_struct_serialization_name_check_skip_requires_enforce_order() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::SerializeRow)]
    /// #[scylla(crate = scylla_cql, skip_name_checks)]
    /// struct TestRow {
    ///     #[scylla(rename = "b")]
    ///     a: i32,
    /// }
    /// ```
    fn _test_struct_serialization_skip_name_check_conflicts_with_rename() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::SerializeRow)]
    /// #[scylla(crate = scylla_cql)]
    /// struct TestRow {
    ///     #[scylla(rename = "b")]
    ///     a: i32,
    ///     b: String,
    /// }
    /// ```
    fn _test_struct_serialization_skip_rename_collision_with_field() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::SerializeRow)]
    /// #[scylla(crate = scylla_cql)]
    /// struct TestRow {
    ///     #[scylla(rename = "c")]
    ///     a: i32,
    ///     #[scylla(rename = "c")]
    ///     b: String,
    /// }
    /// ```
    fn _test_struct_serialization_rename_collision_with_another_rename() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::DeserializeRow)]
    /// #[scylla(crate = scylla_cql, skip_name_checks)]
    /// struct TestRow {}
    /// ```
    fn _test_struct_deserialization_name_check_skip_requires_enforce_order() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::DeserializeRow)]
    /// #[scylla(crate = scylla_cql, skip_name_checks)]
    /// struct TestRow {
    ///     #[scylla(rename = "b")]
    ///     a: i32,
    /// }
    /// ```
    fn _test_struct_deserialization_skip_name_check_conflicts_with_rename() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::DeserializeRow)]
    /// #[scylla(crate = scylla_cql)]
    /// struct TestRow {
    ///     #[scylla(rename = "b")]
    ///     a: i32,
    ///     b: String,
    /// }
    /// ```
    fn _test_struct_deserialization_skip_rename_collision_with_field() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::DeserializeRow)]
    /// #[scylla(crate = scylla_cql)]
    /// struct TestRow {
    ///     #[scylla(rename = "c")]
    ///     a: i32,
    ///     #[scylla(rename = "c")]
    ///     b: String,
    /// }
    /// ```
    fn _test_struct_deserialization_rename_collision_with_another_rename() {}
}

mod value {
    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::SerializeValue)]
    /// #[scylla(crate = scylla_cql, skip_name_checks)]
    /// struct TestUdt {}
    /// ```
    fn _test_serialization_udt_bad_attributes_skip_name_check_requires_enforce_order() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::SerializeValue)]
    /// #[scylla(crate = scylla_cql, flavor = "enforce_order", skip_name_checks)]
    /// struct TestUdt {
    ///     #[scylla(rename = "b")]
    ///     a: i32,
    /// }
    /// ```
    fn _test_serialization_udt_bad_attributes_skip_name_check_conflicts_with_rename() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::SerializeValue)]
    /// #[scylla(crate = scylla_cql)]
    /// struct TestUdt {
    ///     #[scylla(rename = "b")]
    ///     a: i32,
    ///     b: String,
    /// }
    /// ```
    fn _test_serialization_udt_bad_attributes_rename_collision_with_field() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::SerializeValue)]
    /// #[scylla(crate = scylla_cql)]
    /// struct TestUdt {
    ///     #[scylla(rename = "c")]
    ///     a: i32,
    ///     #[scylla(rename = "c")]
    ///     b: String,
    /// }
    /// ```
    fn _test_serialization_udt_bad_attributes_rename_collision_with_another_rename() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::SerializeValue)]
    /// #[scylla(crate = scylla_cql, flavor = "enforce_order", skip_name_checks)]
    /// struct TestUdt {
    ///     a: i32,
    ///     #[scylla(allow_missing)]
    ///     b: bool,
    ///     c: String,
    /// }
    /// ```
    fn _test_serialization_udt_bad_attributes_name_skip_name_checks_limitations_on_allow_missing() {
    }

    /// ```
    ///
    /// #[derive(scylla_macros::SerializeValue)]
    /// #[scylla(crate = scylla_cql, flavor = "enforce_order", skip_name_checks)]
    /// struct TestUdt {
    ///     a: i32,
    ///     #[scylla(allow_missing)]
    ///     b: bool,
    ///     #[scylla(allow_missing)]
    ///     c: String,
    /// }
    /// ```
    fn _test_serialization_udt_good_attributes_name_skip_name_checks_limitations_on_allow_missing()
    {
    }

    /// ```
    /// #[derive(scylla_macros::SerializeValue)]
    /// #[scylla(crate = scylla_cql)]
    /// struct TestUdt {
    ///     a: i32,
    ///     #[scylla(allow_missing)]
    ///     b: bool,
    ///     c: String,
    /// }
    /// ```
    fn _test_serialization_udt_unordered_flavour_no_limitations_on_allow_missing() {}

    /// ```
    /// #[derive(scylla_macros::SerializeValue)]
    /// #[scylla(crate = scylla_cql)]
    /// struct TestUdt {
    ///     a: i32,
    ///     #[scylla(default_when_null)]
    ///     b: bool,
    ///     c: String,
    /// }
    /// ```
    fn _test_serialization_udt_default_when_null_is_accepted() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::DeserializeValue)]
    /// #[scylla(crate = scylla_cql, skip_name_checks)]
    /// struct TestUdt {}
    /// ```
    fn _test_deserialization_udt_bad_attributes_skip_name_check_requires_enforce_order() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::DeserializeValue)]
    /// #[scylla(crate = scylla_cql, flavor = "enforce_order", skip_name_checks)]
    /// struct TestUdt {
    ///     #[scylla(rename = "b")]
    ///     a: i32,
    /// }
    /// ```
    fn _test_deserialization_udt_bad_attributes_skip_name_check_conflicts_with_rename() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::DeserializeValue)]
    /// #[scylla(crate = scylla_cql)]
    /// struct TestUdt {
    ///     #[scylla(rename = "b")]
    ///     a: i32,
    ///     b: String,
    /// }
    /// ```
    fn _test_deserialization_udt_bad_attributes_rename_collision_with_field() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::DeserializeValue)]
    /// #[scylla(crate = scylla_cql)]
    /// struct TestUdt {
    ///     #[scylla(rename = "c")]
    ///     a: i32,
    ///     #[scylla(rename = "c")]
    ///     b: String,
    /// }
    /// ```
    fn _test_deserialization_udt_bad_attributes_rename_collision_with_another_rename() {}

    /// ```compile_fail
    ///
    /// #[derive(scylla_macros::DeserializeValue)]
    /// #[scylla(crate = scylla_cql, flavor = "enforce_order", skip_name_checks)]
    /// struct TestUdt {
    ///     a: i32,
    ///     #[scylla(allow_missing)]
    ///     b: bool,
    ///     c: String,
    /// }
    /// ```
    fn _test_deserialization_udt_bad_attributes_name_skip_name_checks_limitations_on_allow_missing()
    {
    }

    /// ```
    /// #[derive(scylla_macros::DeserializeValue)]
    /// #[scylla(crate = scylla_cql)]
    /// struct TestUdt {
    ///     a: i32,
    ///     #[scylla(allow_missing)]
    ///     b: bool,
    ///     c: String,
    /// }
    /// ```
    fn _test_deserialization_udt_unordered_flavour_no_limitations_on_allow_missing() {}
}
