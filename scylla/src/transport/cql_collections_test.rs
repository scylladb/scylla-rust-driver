use crate::cql_to_rust::FromCqlVal;
use crate::frame::value::Value;
use crate::test_utils::create_new_session_builder;
use crate::utils::test_utils::unique_keyspace_name;
use crate::{frame::response::result::CqlValue, Legacy08Session};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

async fn connect() -> Legacy08Session {
    let session = create_new_session_builder().build_legacy().await.unwrap();
    let ks = unique_keyspace_name();
    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[]).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
}

async fn create_table(session: &Legacy08Session, table_name: &str, value_type: &str) {
    session
        .query(
            format!(
                "CREATE TABLE IF NOT EXISTS {} (p int PRIMARY KEY, val {})",
                table_name, value_type
            ),
            (),
        )
        .await
        .unwrap();
}

async fn insert_and_select<InsertT, SelectT>(
    session: &Legacy08Session,
    table_name: &str,
    to_insert: &InsertT,
    expected: &SelectT,
) where
    InsertT: Value,
    SelectT: FromCqlVal<Option<CqlValue>> + PartialEq + std::fmt::Debug,
{
    session
        .query(
            format!("INSERT INTO {} (p, val) VALUES (0, ?)", table_name),
            (&to_insert,),
        )
        .await
        .unwrap();

    let selected_value: SelectT = session
        .query(format!("SELECT val FROM {} WHERE p = 0", table_name), ())
        .await
        .unwrap()
        .single_row_typed::<(SelectT,)>()
        .unwrap()
        .0;

    assert_eq!(&selected_value, expected);
}

#[tokio::test]
async fn test_cql_list() {
    let session: Legacy08Session = connect().await;

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
    let session: Legacy08Session = connect().await;

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
    let session: Legacy08Session = connect().await;

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
    let session: Legacy08Session = connect().await;

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
