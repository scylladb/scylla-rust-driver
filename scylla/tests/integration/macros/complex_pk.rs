use crate::utils::{
    create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL as _,
};

#[tokio::test]
async fn test_macros_complex_pk() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(ks, true).await.unwrap();
    session
        .ddl("CREATE TABLE IF NOT EXISTS complex_pk (a int, b int, c text, d int, e int, primary key ((a,b,c),d))")
        .await
        .unwrap();

    // Check that SerializeRow and DeserializeRow macros work
    {
        #[derive(scylla::SerializeRow, scylla::DeserializeRow, PartialEq, Debug, Clone)]
        struct ComplexPk {
            a: i32,
            b: i32,
            c: Option<String>,
            d: i32,
            e: i32,
        }
        let input: ComplexPk = ComplexPk {
            a: 9,
            b: 8,
            c: Some("seven".into()),
            d: 6,
            e: 5,
        };
        session
            .query_unpaged(
                "INSERT INTO complex_pk (a,b,c,d,e) VALUES (?,?,?,?,?)",
                input.clone(),
            )
            .await
            .unwrap();
        let output: ComplexPk = session
            .query_unpaged(
                "SELECT * FROM complex_pk WHERE a = 9 and b = 8 and c = 'seven'",
                &[],
            )
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .single_row()
            .unwrap();
        assert_eq!(input, output)
    }
}
