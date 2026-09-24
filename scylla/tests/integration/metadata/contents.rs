use std::collections::HashSet;
use std::env;
use std::sync::Arc;

use itertools::Itertools as _;
use scylla::{
    cluster::metadata::{
        CollectionType, ColumnKind, ColumnType, FunctionSignature, IndexKind, NativeType,
        UserDefinedType,
    },
    value::Row,
};

use crate::utils::{
    PerformDDL as _, create_new_session_builder, disable_tablets_unless_supported,
    scylla_supports_tablets, setup_tracing, unique_keyspace_name,
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
        .ddl(format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"))
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {ks}"), &[])
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

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
async fn test_user_defined_types_in_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"))
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {ks}"), &[])
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

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
async fn test_column_kinds_in_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"))
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {ks}"), &[])
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

    let cluster_state = session.get_cluster_state();
    let columns = &cluster_state.get_keyspace(&ks).unwrap().tables["t"].columns;

    assert_eq!(columns["a"].kind, ColumnKind::Clustering);
    assert_eq!(columns["b"].kind, ColumnKind::Clustering);
    assert_eq!(columns["c"].kind, ColumnKind::PartitionKey);
    assert_eq!(columns["d"].kind, ColumnKind::Static);
    assert_eq!(columns["e"].kind, ColumnKind::PartitionKey);
    assert_eq!(columns["f"].kind, ColumnKind::Regular);

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
async fn test_primary_key_ordering_in_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"))
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {ks}"), &[])
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

    let cluster_state = session.get_cluster_state();
    let table = &cluster_state.get_keyspace(&ks).unwrap().tables["t"];

    assert_eq!(table.partition_key, vec!["c", "e"]);
    assert_eq!(table.clustering_key, vec!["b", "a"]);

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
#[cfg_attr(cassandra_tests, ignore)]
async fn test_table_partitioner_in_metadata() {
    setup_tracing();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    // This test uses CDC, which older ScyllaDB versions do not support on tablet keyspaces.
    let create_ks = format!(
        "CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}{}",
        disable_tablets_unless_supported(&session, "CDC_WITH_TABLETS").await
    );

    session.ddl(create_ks).await.unwrap();

    session
        .query_unpaged(format!("USE {ks}"), &[])
        .await
        .unwrap();

    session
        .ddl(
            "CREATE TABLE t (pk int, ck int, v int, PRIMARY KEY (pk, ck, v))WITH cdc = {'enabled':true}",
        )
        .await
        .unwrap();

    let cluster_state = session.get_cluster_state();
    let tables = &cluster_state.get_keyspace(&ks).unwrap().tables;
    let table = &tables["t"];
    let cdc_table = &tables["t_scylla_cdc_log"];

    assert_eq!(table.partitioner, None);
    assert_eq!(
        cdc_table.partitioner.as_ref().unwrap(),
        "com.scylladb.dht.CDCPartitioner"
    );

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
#[cfg_attr(cassandra_tests, ignore)]
async fn test_views_in_schema_info() {
    let _ = tracing_subscriber::fmt::try_init();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    // This test uses materialized views, which older ScyllaDB versions do not support
    // on tablet keyspaces.
    let create_ks = format!(
        "CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}{}",
        disable_tablets_unless_supported(&session, "VIEWS_WITH_TABLETS").await
    );
    session.ddl(create_ks).await.unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .ddl("CREATE TABLE t(id int PRIMARY KEY, v int)")
        .await
        .unwrap();

    session.ddl("CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM t WHERE v IS NOT NULL PRIMARY KEY (v, id)").await.unwrap();
    session.ddl("CREATE MATERIALIZED VIEW mv2 AS SELECT id, v FROM t WHERE v IS NOT NULL PRIMARY KEY (v, id)").await.unwrap();

    let keyspace_meta = session
        .get_cluster_state()
        .get_keyspace(&ks)
        .unwrap()
        .clone();

    let tables = keyspace_meta
        .tables
        .keys()
        .map(|s| s.as_str())
        .collect::<std::collections::HashSet<&str>>();

    let views = keyspace_meta
        .views
        .keys()
        .map(|s| s.as_str())
        .collect::<std::collections::HashSet<&str>>();
    let views_base_table = keyspace_meta
        .views
        .values()
        .map(|view_meta| view_meta.base_table_name.as_str())
        .collect::<std::collections::HashSet<&str>>();

    assert_eq!(tables, std::collections::HashSet::from(["t"]));
    assert_eq!(views, std::collections::HashSet::from(["mv1", "mv2"]));
    assert_eq!(views_base_table, std::collections::HashSet::from(["t"]));

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
async fn test_indexes_in_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    // A secondary index is backed by a materialized view on ScyllaDB, which older
    // versions do not support on tablet keyspaces.
    let create_ks = format!(
        "CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}{}",
        disable_tablets_unless_supported(&session, "VIEWS_WITH_TABLETS").await
    );
    session.ddl(create_ks).await.unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .ddl("CREATE TABLE t (pk int PRIMARY KEY, a int, b text)")
        .await
        .unwrap();
    session
        .ddl("CREATE TABLE not_indexed (pk int PRIMARY KEY, a int)")
        .await
        .unwrap();
    session.ddl("CREATE INDEX idx_a ON t(a)").await.unwrap();
    session.ddl("CREATE INDEX idx_b ON t(b)").await.unwrap();

    let cluster_state = session.get_cluster_state();
    let keyspace = cluster_state.get_keyspace(&ks).unwrap();

    let indexes = &keyspace.tables["t"].indexes;

    assert_eq!(
        indexes.keys().sorted().collect::<Vec<_>>(),
        vec!["idx_a", "idx_b"]
    );

    let idx_a = &indexes["idx_a"];
    assert_eq!(idx_a.name, "idx_a");
    assert_eq!(idx_a.kind, IndexKind::Composites);

    // A table with no index has empty index metadata, rather than missing from it.
    assert!(keyspace.tables["not_indexed"].indexes.is_empty());

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

// Requires a server with user defined functions enabled, which the ScyllaDB test
// cluster of this repository is started with. UDFs cannot be enabled through the
// environment of the Cassandra test cluster, so the test is skipped there.
#[tokio::test]
#[cfg_attr(cassandra_tests, ignore)]
async fn test_user_defined_functions_and_aggregates_in_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"))
        .await
        .unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();

    // Two overloads of one name, which only a signature tells apart.
    session
        .ddl(
            "CREATE FUNCTION twice(val int) RETURNS NULL ON NULL INPUT RETURNS int \
             LANGUAGE lua AS 'return 2 * val;'",
        )
        .await
        .unwrap();
    session
        .ddl(
            "CREATE FUNCTION twice(val text) CALLED ON NULL INPUT RETURNS text \
             LANGUAGE lua AS 'return val .. val;'",
        )
        .await
        .unwrap();

    // An aggregate built out of a state function and a final function.
    session
        .ddl(
            "CREATE FUNCTION accumulate(acc bigint, val int) CALLED ON NULL INPUT \
             RETURNS bigint LANGUAGE lua AS 'return acc + val;'",
        )
        .await
        .unwrap();
    session
        .ddl(
            "CREATE FUNCTION as_text(acc bigint) CALLED ON NULL INPUT RETURNS text \
             LANGUAGE lua AS 'return tostring(acc);'",
        )
        .await
        .unwrap();
    session
        .ddl(
            "CREATE AGGREGATE sum_as_text(int) SFUNC accumulate STYPE bigint \
             FINALFUNC as_text INITCOND 0",
        )
        .await
        .unwrap();

    // A second aggregate, declaring a reduce function - a ScyllaDB extension, which
    // lets the server compute the aggregate in a distributed manner - and leaving out
    // the final function and the initial condition.
    session
        .ddl(
            "CREATE FUNCTION merge_states(a bigint, b bigint) CALLED ON NULL INPUT \
             RETURNS bigint LANGUAGE lua AS 'return a + b;'",
        )
        .await
        .unwrap();
    session
        .ddl("CREATE AGGREGATE total(int) SFUNC accumulate STYPE bigint REDUCEFUNC merge_states")
        .await
        .unwrap();

    let cluster_state = session.get_cluster_state();
    let keyspace = cluster_state.get_keyspace(&ks).unwrap();

    let functions = &keyspace.user_defined_functions;

    assert_eq!(
        functions
            .keys()
            .map(|signature| (signature.name.as_str(), signature.argument_types.clone()))
            .sorted()
            .collect::<Vec<_>>(),
        vec![
            ("accumulate", vec!["bigint".to_owned(), "int".to_owned()]),
            ("as_text", vec!["bigint".to_owned()]),
            (
                "merge_states",
                vec!["bigint".to_owned(), "bigint".to_owned()]
            ),
            ("twice", vec!["int".to_owned()]),
            ("twice", vec!["text".to_owned()]),
        ]
    );
    assert_eq!(keyspace.functions_named("twice").count(), 2);

    let twice_int = &functions[&FunctionSignature::new("twice".to_owned(), vec!["int".to_owned()])];
    assert_eq!(twice_int.keyspace, ks);
    assert_eq!(twice_int.name, "twice");
    assert_eq!(twice_int.argument_names, vec!["val"]);
    assert_eq!(
        twice_int.argument_types,
        vec![ColumnType::Native(NativeType::Int)]
    );
    assert_eq!(twice_int.return_type, ColumnType::Native(NativeType::Int));
    assert_eq!(twice_int.language, "lua");
    assert_eq!(twice_int.body, "return 2 * val;");
    assert!(!twice_int.called_on_null_input);

    let twice_text =
        &functions[&FunctionSignature::new("twice".to_owned(), vec!["text".to_owned()])];
    assert_eq!(twice_text.return_type, ColumnType::Native(NativeType::Text));
    assert_eq!(twice_text.body, "return val .. val;");
    assert!(twice_text.called_on_null_input);

    let aggregate = &keyspace.user_defined_aggregates
        [&FunctionSignature::new("sum_as_text".to_owned(), vec!["int".to_owned()])];
    assert_eq!(aggregate.keyspace, ks);
    assert_eq!(aggregate.name, "sum_as_text");
    assert_eq!(
        aggregate.argument_types,
        vec![ColumnType::Native(NativeType::Int)]
    );
    assert_eq!(aggregate.return_type, ColumnType::Native(NativeType::Text));
    assert_eq!(aggregate.state_type, ColumnType::Native(NativeType::BigInt));
    assert_eq!(
        aggregate.initial_condition.as_deref(),
        Some("0"),
        "the initial condition is kept as the CQL literal the server stores"
    );
    // No REDUCEFUNC was declared, so there is no reduce function even on ScyllaDB.
    assert_eq!(aggregate.reduce_function, None);

    assert_eq!(functions[&aggregate.state_function].name, "accumulate");
    let final_function = aggregate
        .final_function
        .as_ref()
        .expect("the aggregate declares a final function");
    assert_eq!(functions[final_function].name, "as_text");

    let with_reduce = &keyspace.user_defined_aggregates
        [&FunctionSignature::new("total".to_owned(), vec!["int".to_owned()])];
    // A final function and an initial condition the aggregate does not declare are
    // reported as null by the server, and as `None` here.
    assert_eq!(with_reduce.final_function, None);
    assert_eq!(with_reduce.initial_condition, None);
    let reduce_function = with_reduce
        .reduce_function
        .as_ref()
        .expect("the aggregate declares a reduce function");
    assert_eq!(functions[reduce_function].name, "merge_states");

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// This test case indicates that we support enough CQL types to parse schema keyspace information.
#[tokio::test]
async fn test_fetch_system_keyspace() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let prepared_statement = session
        .prepare("SELECT * FROM system_schema.keyspaces")
        .await
        .unwrap();

    // We materialize all rows as Row, one by one, to assert that we are able of deserializing them.
    session
        .execute_unpaged(&prepared_statement, &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<Row>()
        .unwrap()
        .for_each(|_| ());
}

#[tokio::test]
async fn test_durable_writes_in_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks_durable = unique_keyspace_name();
    let ks_non_durable = unique_keyspace_name();

    // Create a keyspace with durable_writes explicitly set to true
    session
        .ddl(format!(
            "CREATE KEYSPACE {ks_durable} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}} AND DURABLE_WRITES = true"
        ))
        .await
        .unwrap();

    // Create a keyspace with durable_writes explicitly set to false
    session
        .ddl(format!(
            "CREATE KEYSPACE {ks_non_durable} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}} AND DURABLE_WRITES = false"
        ))
        .await
        .unwrap();

    let cluster_state = session.get_cluster_state();

    let ks_durable_meta = cluster_state.get_keyspace(&ks_durable).unwrap();
    assert!(ks_durable_meta.durable_writes);

    let ks_non_durable_meta = cluster_state.get_keyspace(&ks_non_durable).unwrap();
    assert!(!ks_non_durable_meta.durable_writes);

    session
        .ddl(format!("DROP KEYSPACE {ks_durable}"))
        .await
        .unwrap();
    session
        .ddl(format!("DROP KEYSPACE {ks_non_durable}"))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_session_should_have_cluster_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let state = session.get_cluster_state();

    let expected_addresses: HashSet<String> = [
        env::var("SCYLLA_URI").unwrap_or_else(|_| "172.42.0.2:9042".to_string()),
        env::var("SCYLLA_URI2").unwrap_or_else(|_| "172.42.0.3:9042".to_string()),
        env::var("SCYLLA_URI3").unwrap_or_else(|_| "172.42.0.4:9042".to_string()),
    ]
    .into_iter()
    .collect();

    let got_addresses: HashSet<String> = state
        .get_nodes_info()
        .iter()
        .map(|node| node.address.to_string())
        .collect();

    assert_eq!(
        got_addresses, expected_addresses,
        "Cluster node addresses do not match environment variables"
    );

    assert_eq!(state.cluster_name(), "TestCluster");
}

#[tokio::test]
#[cfg_attr(cassandra_tests, ignore)]
async fn test_tablets_enabled_in_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    // This test only makes sense on ScyllaDB versions that support tablets.
    if !scylla_supports_tablets(&session).await {
        return;
    }

    let ks = unique_keyspace_name();

    // Create a keyspace with tablets explicitly enabled.
    session
        .ddl(format!(
            "CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}} AND TABLETS = {{'enabled': true}}"
        ))
        .await
        .unwrap();

    let cluster_state = session.get_cluster_state();
    let ks_meta = cluster_state.get_keyspace(&ks).unwrap();
    assert!(ks_meta.tablet_based);

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
async fn test_tablets_disabled_in_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    // Create a keyspace with tablets explicitly disabled on ScyllaDB.
    // On Cassandra (and old ScyllaDB versions without tablets support) the
    // keyspace simply has no tablets, so we don't add the TABLETS option.
    let mut create_ks = format!(
        "CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
    );
    if scylla_supports_tablets(&session).await {
        create_ks += " AND TABLETS = {'enabled': false}";
    }

    session.ddl(create_ks).await.unwrap();

    let cluster_state = session.get_cluster_state();
    let ks_meta = cluster_state.get_keyspace(&ks).unwrap();
    assert!(!ks_meta.tablet_based);

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}
