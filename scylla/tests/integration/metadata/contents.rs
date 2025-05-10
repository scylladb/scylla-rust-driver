use std::sync::Arc;

use itertools::Itertools as _;
use scylla::cluster::metadata::{
    CollectionType, ColumnKind, ColumnType, NativeType, UserDefinedType,
};

use crate::utils::{
    create_new_session_builder, scylla_supports_tablets, setup_tracing, unique_keyspace_name,
    PerformDDL as _,
};

fn udt_type_a_def(ks: &str) -> Arc<UserDefinedType<'_>> {
    Arc::new(UserDefinedType {
        name: "type_a".into(),
        keyspace: ks.into(),
        field_types: vec![
            (
                "a".into(),
                ColumnType::Collection {
                    frozen: false,
                    typ: CollectionType::Map(
                        Box::new(ColumnType::Collection {
                            frozen: true,
                            typ: CollectionType::List(Box::new(ColumnType::Native(
                                NativeType::Int,
                            ))),
                        }),
                        Box::new(ColumnType::Native(NativeType::Text)),
                    ),
                },
            ),
            (
                "b".into(),
                ColumnType::Collection {
                    frozen: true,
                    typ: CollectionType::Map(
                        Box::new(ColumnType::Collection {
                            frozen: true,
                            typ: CollectionType::List(Box::new(ColumnType::Native(
                                NativeType::Int,
                            ))),
                        }),
                        Box::new(ColumnType::Collection {
                            frozen: true,
                            typ: CollectionType::Set(Box::new(ColumnType::Native(
                                NativeType::Text,
                            ))),
                        }),
                    ),
                },
            ),
        ],
    })
}

fn udt_type_b_def(ks: &str) -> Arc<UserDefinedType<'_>> {
    Arc::new(UserDefinedType {
        name: "type_b".into(),
        keyspace: ks.into(),
        field_types: vec![
            ("a".into(), ColumnType::Native(NativeType::Int)),
            ("b".into(), ColumnType::Native(NativeType::Text)),
        ],
    })
}

fn udt_type_c_def(ks: &str) -> Arc<UserDefinedType<'_>> {
    Arc::new(UserDefinedType {
        name: "type_c".into(),
        keyspace: ks.into(),
        field_types: vec![(
            "a".into(),
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(
                    Box::new(ColumnType::Collection {
                        frozen: true,
                        typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Text))),
                    }),
                    Box::new(ColumnType::UserDefinedType {
                        frozen: true,
                        definition: udt_type_b_def(ks),
                    }),
                ),
            },
        )],
    })
}

#[tokio::test]
async fn test_schema_types_in_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks))
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {}", ks), &[])
        .await
        .unwrap();

    session
        .ddl(
            "CREATE TYPE IF NOT EXISTS type_a (
                    a map<frozen<list<int>>, text>,
                    b frozen<map<frozen<list<int>>, frozen<set<text>>>>
                   )",
        )
        .await
        .unwrap();

    session
        .ddl("CREATE TYPE IF NOT EXISTS type_b (a int, b text)")
        .await
        .unwrap();

    session
        .ddl("CREATE TYPE IF NOT EXISTS type_c (a map<frozen<set<text>>, frozen<type_b>>)")
        .await
        .unwrap();

    session
        .ddl(
            "CREATE TABLE IF NOT EXISTS table_a (
                    a frozen<type_a> PRIMARY KEY,
                    b type_b,
                    c frozen<type_c>,
                    d map<text, frozen<list<int>>>,
                    e tuple<int, text>
                  )",
        )
        .await
        .unwrap();

    session
        .ddl(
            "CREATE TABLE IF NOT EXISTS table_b (
                        a text PRIMARY KEY,
                        b frozen<map<int, int>>
                     )",
        )
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let cluster_state = session.get_cluster_state();
    let tables = &cluster_state.get_keyspace(&ks).unwrap().tables;

    assert_eq!(
        tables.keys().sorted().collect::<Vec<_>>(),
        vec!["table_a", "table_b"]
    );

    let table_a_columns = &tables["table_a"].columns;

    assert_eq!(
        table_a_columns.keys().sorted().collect::<Vec<_>>(),
        vec!["a", "b", "c", "d", "e"]
    );

    let a = &table_a_columns["a"];

    assert_eq!(
        a.typ,
        ColumnType::UserDefinedType {
            frozen: true,
            definition: udt_type_a_def(&ks),
        }
    );

    let b = &table_a_columns["b"];

    assert_eq!(
        b.typ,
        ColumnType::UserDefinedType {
            frozen: false,
            definition: udt_type_b_def(&ks),
        }
    );

    let c = &table_a_columns["c"];

    assert_eq!(
        c.typ,
        ColumnType::UserDefinedType {
            frozen: true,
            definition: udt_type_c_def(&ks)
        }
    );

    let d = &table_a_columns["d"];

    assert_eq!(
        d.typ,
        ColumnType::Collection {
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Text)),
                Box::new(ColumnType::Collection {
                    typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int))),
                    frozen: true
                })
            ),
            frozen: false
        }
    );

    let e = &table_a_columns["e"];

    assert_eq!(
        e.typ,
        ColumnType::Tuple(vec![
            ColumnType::Native(NativeType::Int),
            ColumnType::Native(NativeType::Text)
        ],)
    );

    let table_b_columns = &tables["table_b"].columns;

    let a = &table_b_columns["a"];

    assert_eq!(a.typ, ColumnType::Native(NativeType::Text));

    let b = &table_b_columns["b"];

    assert_eq!(
        b.typ,
        ColumnType::Collection {
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Int),),
                Box::new(ColumnType::Native(NativeType::Int),)
            ),
            frozen: true
        }
    );
}

#[tokio::test]
async fn test_user_defined_types_in_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks))
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {}", ks), &[])
        .await
        .unwrap();

    session
        .ddl(
            "CREATE TYPE IF NOT EXISTS type_a (
                    a map<frozen<list<int>>, text>,
                    b frozen<map<frozen<list<int>>, frozen<set<text>>>>
                   )",
        )
        .await
        .unwrap();

    session
        .ddl("CREATE TYPE IF NOT EXISTS type_b (a int, b text)")
        .await
        .unwrap();

    session
        .ddl("CREATE TYPE IF NOT EXISTS type_c (a map<frozen<set<text>>, frozen<type_b>>)")
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let cluster_state = session.get_cluster_state();
    let user_defined_types = &cluster_state.get_keyspace(&ks).unwrap().user_defined_types;

    assert_eq!(
        user_defined_types.keys().sorted().collect::<Vec<_>>(),
        vec!["type_a", "type_b", "type_c"]
    );

    let type_a = &user_defined_types["type_a"];

    assert_eq!(*type_a, udt_type_a_def(&ks));

    let type_b = &user_defined_types["type_b"];

    assert_eq!(*type_b, udt_type_b_def(&ks));

    let type_c = &user_defined_types["type_c"];

    assert_eq!(*type_c, udt_type_c_def(&ks));
}

#[tokio::test]
async fn test_column_kinds_in_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks))
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {}", ks), &[])
        .await
        .unwrap();

    session
        .ddl(
            "CREATE TABLE IF NOT EXISTS t (
                    a int,
                    b int,
                    c int,
                    d int STATIC,
                    e int,
                    f int,
                    PRIMARY KEY ((c, e), b, a)
                  )",
        )
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let cluster_state = session.get_cluster_state();
    let columns = &cluster_state.get_keyspace(&ks).unwrap().tables["t"].columns;

    assert_eq!(columns["a"].kind, ColumnKind::Clustering);
    assert_eq!(columns["b"].kind, ColumnKind::Clustering);
    assert_eq!(columns["c"].kind, ColumnKind::PartitionKey);
    assert_eq!(columns["d"].kind, ColumnKind::Static);
    assert_eq!(columns["e"].kind, ColumnKind::PartitionKey);
    assert_eq!(columns["f"].kind, ColumnKind::Regular);
}

#[tokio::test]
async fn test_primary_key_ordering_in_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks))
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {}", ks), &[])
        .await
        .unwrap();

    session
        .ddl(
            "CREATE TABLE IF NOT EXISTS t (
                    a int,
                    b int,
                    c int,
                    d int STATIC,
                    e int,
                    f int,
                    g int,
                    h int,
                    i int STATIC,
                    PRIMARY KEY ((c, e), b, a)
                  )",
        )
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let cluster_state = session.get_cluster_state();
    let table = &cluster_state.get_keyspace(&ks).unwrap().tables["t"];

    assert_eq!(table.partition_key, vec!["c", "e"]);
    assert_eq!(table.clustering_key, vec!["b", "a"]);
}

#[tokio::test]
async fn test_table_partitioner_in_metadata() {
    setup_tracing();
    if option_env!("CDC") == Some("disabled") {
        return;
    }

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    // This test uses CDC which is not yet compatible with Scylla's tablets.
    let mut create_ks = format!(
        "CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
    );
    if scylla_supports_tablets(&session).await {
        create_ks += " AND TABLETS = {'enabled': false}";
    }

    session.ddl(create_ks).await.unwrap();

    session
        .query_unpaged(format!("USE {}", ks), &[])
        .await
        .unwrap();

    session
        .ddl(
            "CREATE TABLE t (pk int, ck int, v int, PRIMARY KEY (pk, ck, v))WITH cdc = {'enabled':true}",
        )
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let cluster_state = session.get_cluster_state();
    let tables = &cluster_state.get_keyspace(&ks).unwrap().tables;
    let table = &tables["t"];
    let cdc_table = &tables["t_scylla_cdc_log"];

    assert_eq!(table.partitioner, None);
    assert_eq!(
        cdc_table.partitioner.as_ref().unwrap(),
        "com.scylladb.dht.CDCPartitioner"
    );
}

#[tokio::test]
async fn test_views_in_schema_info() {
    let _ = tracing_subscriber::fmt::try_init();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    let mut create_ks = format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks);
    // Materialized views + tablets are not supported in Scylla 2025.1.
    if scylla_supports_tablets(&session).await {
        create_ks += " and TABLETS = { 'enabled': false}";
    }
    session.ddl(create_ks).await.unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .ddl("CREATE TABLE t(id int PRIMARY KEY, v int)")
        .await
        .unwrap();

    session.ddl("CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM t WHERE v IS NOT NULL PRIMARY KEY (v, id)").await.unwrap();
    session.ddl("CREATE MATERIALIZED VIEW mv2 AS SELECT id, v FROM t WHERE v IS NOT NULL PRIMARY KEY (v, id)").await.unwrap();

    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let keyspace_meta = session
        .get_cluster_state()
        .get_keyspace(&ks)
        .unwrap()
        .clone();

    let tables = keyspace_meta
        .tables
        .keys()
        .collect::<std::collections::HashSet<&String>>();

    let views = keyspace_meta
        .views
        .keys()
        .collect::<std::collections::HashSet<&String>>();
    let views_base_table = keyspace_meta
        .views
        .values()
        .map(|view_meta| &view_meta.base_table_name)
        .collect::<std::collections::HashSet<&String>>();

    assert_eq!(tables, std::collections::HashSet::from([&"t".to_string()]));
    assert_eq!(
        views,
        std::collections::HashSet::from([&"mv1".to_string(), &"mv2".to_string()])
    );
    assert_eq!(
        views_base_table,
        std::collections::HashSet::from([&"t".to_string()])
    )
}
