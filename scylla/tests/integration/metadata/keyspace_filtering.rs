use crate::utils::{
    create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL as _,
};

#[tokio::test]
async fn test_keyspaces_to_fetch() {
    setup_tracing();

    let ks1 = unique_keyspace_name();
    let ks2 = unique_keyspace_name();

    let session_default = create_new_session_builder().build().await.unwrap();
    for ks in [&ks1, &ks2] {
        session_default
            .ddl(format!("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks))
            .await
            .unwrap();
    }
    session_default.await_schema_agreement().await.unwrap();
    assert!(session_default
        .get_cluster_state()
        .get_keyspace(&ks1)
        .is_some());
    assert!(session_default
        .get_cluster_state()
        .get_keyspace(&ks2)
        .is_some());

    let session1 = create_new_session_builder()
        .keyspaces_to_fetch([&ks1])
        .build()
        .await
        .unwrap();
    assert!(session1.get_cluster_state().get_keyspace(&ks1).is_some());
    assert!(session1.get_cluster_state().get_keyspace(&ks2).is_none());

    let session_all = create_new_session_builder()
        .keyspaces_to_fetch([] as [String; 0])
        .build()
        .await
        .unwrap();
    assert!(session_all.get_cluster_state().get_keyspace(&ks1).is_some());
    assert!(session_all.get_cluster_state().get_keyspace(&ks2).is_some());
}
