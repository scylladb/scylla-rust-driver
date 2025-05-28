use crate::utils::{
    create_new_session_builder, setup_tracing, unique_keyspace_name, DeserializeOwnedValue,
    PerformDDL,
};
use scylla::cluster::metadata::NativeType;
use scylla::deserialize::value::DeserializeValue;
use scylla::value::{
    CqlDate, CqlDecimal, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlValue, CqlVarint,
};
use scylla::{client::session::Session, cluster::metadata::ColumnType};
use scylla_cql::serialize::value::SerializeValue;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr as _;
use uuid::Uuid;

async fn connect() -> Session {
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
}

async fn create_table(session: &Session, table_name: &str, value_type: &str) {
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {} (p int PRIMARY KEY, val {})",
            table_name, value_type
        ))
        .await
        .unwrap();
}

async fn insert_and_select<InsertT, SelectT>(
    session: &Session,
    table_name: &str,
    to_insert: &InsertT,
    expected: &SelectT,
) where
    InsertT: SerializeValue,
    SelectT: DeserializeOwnedValue + PartialEq + Debug,
{
    session
        .query_unpaged(
            format!("INSERT INTO {} (p, val) VALUES (0, ?)", table_name),
            (&to_insert,),
        )
        .await
        .unwrap();

    let selected_value: SelectT = session
        .query_unpaged(format!("SELECT val FROM {} WHERE p = 0", table_name), ())
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .single_row::<(SelectT,)>()
        .unwrap()
        .0;

    assert_eq!(&selected_value, expected);
}

#[tokio::test]
async fn test_cql_list() {
    setup_tracing();
    let session: Session = connect().await;

    let table_name: &str = "test_cql_list_tab";
    create_table(&session, table_name, "list<int>").await;

    // Vec
    let list1: Vec<i32> = vec![-1, 0, 1, 1, 2];
    let list2: Vec<i32> = vec![100, 212, 2323];
    let list_empty: Vec<i32> = vec![];
    let list_empty_selected: Option<Vec<i32>> = None;
    insert_and_select(&session, table_name, &list1, &list1).await;
    insert_and_select(&session, table_name, &list2, &list2).await;
    insert_and_select(&session, table_name, &list_empty, &list_empty_selected).await;

    // CqlValue
    let list_cql_value: CqlValue =
        CqlValue::List(vec![CqlValue::Int(-1), CqlValue::Int(1), CqlValue::Int(0)]);
    let list_cql_value_empty: CqlValue = CqlValue::List(Vec::new());
    let list_cql_value_empty_selected: Option<CqlValue> = None;

    insert_and_select(&session, table_name, &list_cql_value, &list_cql_value).await;
    insert_and_select(
        &session,
        table_name,
        &list_cql_value_empty,
        &list_cql_value_empty_selected,
    )
    .await;
}

#[tokio::test]
async fn test_cql_set() {
    setup_tracing();
    let session: Session = connect().await;

    let table_name: &str = "test_cql_set_tab";
    create_table(&session, table_name, "set<int>").await;

    // Vec
    let set_vec_sorted: Vec<i32> = vec![-1, 0, 1, 2];
    let set_vec_unordered: Vec<i32> = vec![1, -1, 1, 2, 0];
    let set_vec_empty: Vec<i32> = vec![];
    let set_vec_empty_selected: Option<Vec<i32>> = None;
    insert_and_select(&session, table_name, &set_vec_sorted, &set_vec_sorted).await;
    insert_and_select(&session, table_name, &set_vec_unordered, &set_vec_sorted).await;
    insert_and_select(
        &session,
        table_name,
        &set_vec_empty,
        &set_vec_empty_selected,
    )
    .await;

    // HashSet
    let set_hashset: HashSet<i32> = vec![-1, 1, -2].into_iter().collect();
    let set_hashset_empty: HashSet<i32> = HashSet::new();
    let set_hashset_empty_selected: Option<HashSet<i32>> = None;
    insert_and_select(&session, table_name, &set_hashset, &set_hashset).await;
    insert_and_select(
        &session,
        table_name,
        &set_hashset_empty,
        &set_hashset_empty_selected,
    )
    .await;

    // BTreeSet
    let set_btreeset: BTreeSet<i32> = vec![0, -2, -1].into_iter().collect();
    let set_btreeset_empty: BTreeSet<i32> = BTreeSet::new();
    let set_btreeset_empty_selected: Option<BTreeSet<i32>> = None;
    insert_and_select(&session, table_name, &set_btreeset, &set_btreeset).await;
    insert_and_select(
        &session,
        table_name,
        &set_btreeset_empty,
        &set_btreeset_empty_selected,
    )
    .await;

    // CqlValue
    let set_cql_value: CqlValue =
        CqlValue::Set(vec![CqlValue::Int(-1), CqlValue::Int(1), CqlValue::Int(2)]);
    let set_cql_value_empty: CqlValue = CqlValue::Set(Vec::new());
    let set_cql_value_empty_selected: Option<CqlValue> = None;
    insert_and_select(&session, table_name, &set_cql_value, &set_cql_value).await;
    insert_and_select(
        &session,
        table_name,
        &set_cql_value_empty,
        &set_cql_value_empty_selected,
    )
    .await;
}

#[tokio::test]
async fn test_cql_map() {
    setup_tracing();
    let session: Session = connect().await;

    let table_name: &str = "test_cql_map_tab";
    create_table(&session, table_name, "map<int, int>").await;

    // HashMap
    let map_hashmap: HashMap<i32, i32> = vec![(-1, 0), (0, 1), (2, 1)].into_iter().collect();
    let map_hashmap_empty: HashMap<i32, i32> = HashMap::new();
    let map_hashmap_empty_selected: Option<HashMap<i32, i32>> = None;
    insert_and_select(&session, table_name, &map_hashmap, &map_hashmap).await;
    insert_and_select(
        &session,
        table_name,
        &map_hashmap_empty,
        &map_hashmap_empty_selected,
    )
    .await;

    // BTreeMap
    let map_btreemap: BTreeMap<i32, i32> = vec![(10, 20), (30, 10), (4, 5)].into_iter().collect();
    let map_btreemap_empty: BTreeMap<i32, i32> = BTreeMap::new();
    let map_btreemap_empty_selected: Option<BTreeMap<i32, i32>> = None;
    insert_and_select(&session, table_name, &map_btreemap, &map_btreemap).await;
    insert_and_select(
        &session,
        table_name,
        &map_btreemap_empty,
        &map_btreemap_empty_selected,
    )
    .await;

    // CqlValue
    let map_cql_value: CqlValue = CqlValue::Map(vec![
        (CqlValue::Int(2), CqlValue::Int(4)),
        (CqlValue::Int(8), CqlValue::Int(16)),
    ]);
    let map_cql_value_empty: CqlValue = CqlValue::Map(Vec::new());
    let map_cql_value_empty_selected: Option<CqlValue> = None;
    insert_and_select(&session, table_name, &map_cql_value, &map_cql_value).await;
    insert_and_select(
        &session,
        table_name,
        &map_cql_value_empty,
        &map_cql_value_empty_selected,
    )
    .await;
}

#[tokio::test]
async fn test_cql_tuple() {
    setup_tracing();
    let session: Session = connect().await;

    let table_name: &str = "test_cql_tuple_tab";
    create_table(&session, table_name, "tuple<int, int, text>").await;

    // Rust tuple ()
    let tuple1: (i32, i32, String) = (1, 2, "three".to_string());
    let tuple2: (Option<i32>, Option<i32>, String) = (Some(4), None, "sixteen".to_string());
    insert_and_select(&session, table_name, &tuple1, &tuple1).await;
    insert_and_select(&session, table_name, &tuple2, &tuple2).await;

    // CqlValue
    let tuple_cql_value: CqlValue = CqlValue::Tuple(vec![
        None,
        Some(CqlValue::Int(1024)),
        Some(CqlValue::Text("cql_value_text".to_string())),
    ]);
    insert_and_select(&session, table_name, &tuple_cql_value, &tuple_cql_value).await;
}

// TODO: Remove this ignore when vector type is supported in ScyllaDB
#[cfg_attr(not(cassandra_tests), ignore)]
#[tokio::test]
async fn test_vector_type_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session
        .ddl(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.t (a int PRIMARY KEY, b vector<int, 4>, c vector<text, 2>)",
                ks
            ),
        )
        .await
        .unwrap();

    session.refresh_metadata().await.unwrap();
    let metadata = session.get_cluster_state();
    let columns = &metadata.get_keyspace(&ks).unwrap().tables["t"].columns;
    assert_eq!(
        columns["b"].typ,
        ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Int)),
            dimensions: 4,
        },
    );
    assert_eq!(
        columns["c"].typ,
        ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Text)),
            dimensions: 2,
        },
    );
}

// TODO: Remove this ignore when vector type is supported in ScyllaDB
#[cfg_attr(not(cassandra_tests), ignore)]
#[tokio::test]
async fn test_vector_type_unprepared() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session
        .ddl(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.t (a int PRIMARY KEY, b vector<int, 4>, c vector<text, 2>)",
                ks
            ),
        )
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO {}.t (a, b, c) VALUES (?, ?, ?)", ks),
            &(1, vec![1, 2, 3, 4], vec!["foo", "bar"]),
        )
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO {}.t (a, b, c) VALUES (?, ?, ?)", ks),
            &(2, &[5, 6, 7, 8][..], &["afoo", "abar"][..]),
        )
        .await
        .unwrap();

    let query_result = session
        .query_unpaged(format!("SELECT a, b, c FROM {}.t", ks), &[])
        .await
        .unwrap();

    let rows: Vec<(i32, Vec<i32>, Vec<String>)> = query_result
        .into_rows_result()
        .unwrap()
        .rows::<(i32, Vec<i32>, Vec<String>)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        rows[0],
        (
            1,
            vec![1, 2, 3, 4],
            vec!["foo".to_string(), "bar".to_string()]
        )
    );
    assert_eq!(
        rows[1],
        (
            2,
            vec![5, 6, 7, 8],
            vec!["afoo".to_string(), "abar".to_string()]
        )
    );
}

// TODO: Remove this ignore when vector type is supported in ScyllaDB
#[cfg_attr(not(cassandra_tests), ignore)]
#[tokio::test]
async fn test_vector_type_prepared() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session
        .ddl(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.t (a int PRIMARY KEY, b vector<int, 4>, c vector<text, 2>)",
                ks
            ),
        )
        .await
        .unwrap();

    let prepared_statement = session
        .prepare(format!("INSERT INTO {}.t (a, b, c) VALUES (?, ?, ?)", ks))
        .await
        .unwrap();
    session
        .execute_unpaged(
            &prepared_statement,
            &(2, vec![11, 12, 13, 14], vec!["afoo", "abar"]),
        )
        .await
        .unwrap();

    let query_result = session
        .query_unpaged(format!("SELECT a, b, c FROM {}.t", ks), &[])
        .await
        .unwrap();

    let row = query_result
        .into_rows_result()
        .unwrap()
        .single_row::<(i32, Vec<i32>, Vec<String>)>()
        .unwrap();
    assert_eq!(
        row,
        (
            2,
            vec![11, 12, 13, 14],
            vec!["afoo".to_string(), "abar".to_string()]
        )
    );
}

async fn test_vector_single_type<
    T: SerializeValue + for<'a> DeserializeValue<'a, 'a> + PartialEq + Debug + Clone,
>(
    keyspace: &str,
    session: &Session,
    type_name: &str,
    values: Vec<T>,
) {
    let table_name = format!(
        "test_vector_{}",
        type_name
            .replace("<", "A")
            .replace(">", "B")
            .replace(",", "C")
    );
    let create_statement = format!(
        "CREATE TABLE {}.{} (a int PRIMARY KEY, b vector<{}, {}>)",
        keyspace,
        table_name,
        type_name,
        values.len()
    );
    session.ddl(create_statement).await.unwrap();

    let prepared_insert = session
        .prepare(format!(
            "INSERT INTO {}.{} (a, b) VALUES (?, ?)",
            keyspace, table_name
        ))
        .await
        .unwrap();
    let prepared_select = session
        .prepare(format!("SELECT a, b FROM {}.{}", keyspace, table_name))
        .await
        .unwrap();
    session
        .execute_unpaged(&prepared_insert, &(1, &values))
        .await
        .unwrap();

    let query_result = session
        .execute_unpaged(&prepared_select, &[])
        .await
        .unwrap();

    let result = query_result.into_rows_result().unwrap();

    let row = result.single_row::<(i32, Vec<T>)>().unwrap();

    assert_eq!(row, (1, values));

    let drop_statement = format!("DROP TABLE {}.{}", keyspace, table_name);
    session.ddl(drop_statement).await.unwrap();
}

// TODO: Remove this ignore when vector type is available in ScyllaDB
#[cfg_attr(not(cassandra_tests), ignore)]
#[tokio::test]
async fn test_vector_type_all_types() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();

    // Native types

    test_vector_single_type(
        &ks,
        &session,
        "ascii",
        vec!["foo".to_string(), "bar".to_string()],
    )
    .await;
    test_vector_single_type(&ks, &session, "bigint", vec![1i64, 2i64, 3i64]).await;
    test_vector_single_type(
        &ks,
        &session,
        "blob",
        vec![vec![1_u8, 2_u8], vec![3_u8, 4_u8]],
    )
    .await;
    test_vector_single_type(&ks, &session, "boolean", vec![true, false]).await;
    test_vector_single_type(&ks, &session, "date", vec![CqlDate(123), CqlDate(456)]).await;
    test_vector_single_type(
        &ks,
        &session,
        "decimal",
        vec![
            CqlDecimal::from_signed_be_bytes_slice_and_exponent(b"123", 42),
            CqlDecimal::from_signed_be_bytes_slice_and_exponent(b"123", 42),
        ],
    )
    .await;
    test_vector_single_type(&ks, &session, "double", vec![1.0f64, 2.0f64, 3.0f64]).await;
    test_vector_single_type(
        &ks,
        &session,
        "duration",
        vec![
            CqlDuration {
                months: 1,
                days: 2,
                nanoseconds: 3,
            },
            CqlDuration {
                months: 1,
                days: 2,
                nanoseconds: 3,
            },
        ],
    )
    .await;
    test_vector_single_type(&ks, &session, "float", vec![1.0f32, 2.0f32, 3.0f32]).await;
    test_vector_single_type(
        &ks,
        &session,
        "inet",
        vec![
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
        ],
    )
    .await;
    test_vector_single_type(&ks, &session, "int", vec![1, 2, 3]).await;
    test_vector_single_type(&ks, &session, "smallint", vec![1_i16, 2_i16]).await;
    test_vector_single_type(
        &ks,
        &session,
        "text",
        vec!["foo".to_string(), "bar".to_string()],
    )
    .await;
    test_vector_single_type(&ks, &session, "time", vec![CqlTime(1234), CqlTime(5678)]).await;
    test_vector_single_type(
        &ks,
        &session,
        "timestamp",
        vec![CqlTimestamp(1234), CqlTimestamp(5678)],
    )
    .await;
    test_vector_single_type(
        &ks,
        &session,
        "timeuuid",
        vec![
            CqlTimeuuid::from_str("f4a7c45e-220e-11f0-8a95-325096b39f47").unwrap(),
            CqlTimeuuid::from_str("f4a7c620-220e-11f0-bfde-325096b39f47").unwrap(),
        ],
    )
    .await;
    test_vector_single_type(&ks, &session, "tinyint", vec![1_i8, 2_i8, 3_i8, 4_i8]).await;
    test_vector_single_type(&ks, &session, "uuid", vec![Uuid::new_v4(), Uuid::new_v4()]).await;

    test_vector_single_type(
        &ks,
        &session,
        "varchar",
        vec!["foo".to_string(), "bar".to_string()],
    )
    .await;
    test_vector_single_type(
        &ks,
        &session,
        "varint",
        vec![
            CqlVarint::from_signed_bytes_be(vec![1, 2]),
            CqlVarint::from_signed_bytes_be(vec![3, 4]),
        ],
    )
    .await;

    // Collections

    test_vector_single_type(&ks, &session, "list<int>", vec![vec![1, 2], vec![3, 4]]).await;

    test_vector_single_type(
        &ks,
        &session,
        "set<int>",
        vec![HashSet::from([1, 2]), HashSet::from([3, 4])],
    )
    .await;

    test_vector_single_type(
        &ks,
        &session,
        "map<int,text>",
        vec![
            HashMap::from([(1, "foo".to_string()), (2, "bar".to_string())]),
            HashMap::from([(3, "baz".to_string()), (4, "qux".to_string())]),
        ],
    )
    .await;

    // Tuples

    test_vector_single_type(
        &ks,
        &session,
        "tuple<int,text>",
        vec![
            (1, "foo".to_string()),
            (2, "bar".to_string()),
            (3, "baz".to_string()),
        ],
    )
    .await;

    // Nested vectors
    test_vector_single_type(&ks, &session, "vector<int,3>", vec![vec![1, 2, 3]]).await;

    test_vector_single_type(
        &ks,
        &session,
        "list<vector<int,2>>",
        vec![vec![vec![1, 2], vec![3, 4]], vec![vec![5, 6], vec![7, 8]]],
    )
    .await;
}

/// ScyllaDB does not distinguish empty collections from nulls. That is, INSERTing an empty collection
/// is equivalent to nullifying the corresponding column.
/// As pointed out in [#1001](https://github.com/scylladb/scylla-rust-driver/issues/1001), it's a nice
/// QOL feature to be able to deserialize empty CQL collections to empty Rust collections instead of
/// `None::<RustCollection>`. This test checks that.
#[tokio::test]
async fn test_deserialize_empty_collections() {
    // Setup session.
    let ks = unique_keyspace_name();
    let session = create_new_session_builder().build().await.unwrap();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session.use_keyspace(&ks, true).await.unwrap();

    async fn deserialize_empty_collection<
        Collection: Default + DeserializeOwnedValue + SerializeValue,
    >(
        session: &Session,
        collection_name: &str,
        collection_type_params: &str,
    ) -> Collection {
        // Create a table for the given collection type.
        let table_name = "test_empty_".to_owned() + collection_name;
        let query = format!(
            "CREATE TABLE {} (n int primary key, c {}<{}>)",
            table_name, collection_name, collection_type_params
        );
        session.ddl(query).await.unwrap();

        // Populate the table with an empty collection, effectively inserting null as the collection.
        session
            .query_unpaged(
                format!("INSERT INTO {} (n, c) VALUES (?, ?)", table_name,),
                (0, Collection::default()),
            )
            .await
            .unwrap();

        let query_rows_result = session
            .query_unpaged(format!("SELECT c FROM {}", table_name), ())
            .await
            .unwrap()
            .into_rows_result()
            .unwrap();
        let (collection,) = query_rows_result.first_row::<(Collection,)>().unwrap();

        // Drop the table
        collection
    }

    let list = deserialize_empty_collection::<Vec<i32>>(&session, "list", "int").await;
    assert!(list.is_empty());

    let set = deserialize_empty_collection::<HashSet<i64>>(&session, "set", "bigint").await;
    assert!(set.is_empty());

    let map = deserialize_empty_collection::<HashMap<bool, CqlVarint>>(
        &session,
        "map",
        "boolean, varint",
    )
    .await;
    assert!(map.is_empty());
}
