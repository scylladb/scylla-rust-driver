use scylla::{
    client::session::Session,
    errors::{BadKeyspaceName, UseKeyspaceError},
};

use crate::utils::{
    create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL as _,
};

#[tokio::test]
async fn test_use_keyspace() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();

    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {}.tab (a text primary key)",
            ks
        ))
        .await
        .unwrap();

    session
        .query_unpaged(format!("INSERT INTO {}.tab (a) VALUES ('test1')", ks), &[])
        .await
        .unwrap();

    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .query_unpaged("INSERT INTO tab (a) VALUES ('test2')", &[])
        .await
        .unwrap();

    let mut rows: Vec<String> = session
        .query_unpaged("SELECT * FROM tab", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(String,)>()
        .unwrap()
        .map(|res| res.unwrap().0)
        .collect();

    rows.sort();

    assert_eq!(rows, vec!["test1".to_string(), "test2".to_string()]);

    // Test that trying to use nonexisting keyspace fails
    assert!(session
        .use_keyspace("this_keyspace_does_not_exist_at_all", false)
        .await
        .is_err());

    // Test that invalid keyspaces get rejected
    assert!(matches!(
        session.use_keyspace("", false).await,
        Err(UseKeyspaceError::BadKeyspaceName(BadKeyspaceName::Empty))
    ));

    let long_name: String = ['a'; 49].iter().collect();
    assert!(matches!(
        session.use_keyspace(long_name, false).await,
        Err(UseKeyspaceError::BadKeyspaceName(BadKeyspaceName::TooLong(
            _,
            _
        )))
    ));

    assert!(matches!(
        session.use_keyspace("abcd;dfdsf", false).await,
        Err(UseKeyspaceError::BadKeyspaceName(
            BadKeyspaceName::IllegalCharacter(_, ';')
        ))
    ));

    // Make sure that use_keyspace on SessionBuiler works
    let session2: Session = create_new_session_builder()
        .use_keyspace(ks.clone(), false)
        .build()
        .await
        .unwrap();

    let mut rows2: Vec<String> = session2
        .query_unpaged("SELECT * FROM tab", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(String,)>()
        .unwrap()
        .map(|res| res.unwrap().0)
        .collect();

    rows2.sort();

    assert_eq!(rows2, vec!["test1".to_string(), "test2".to_string()]);
}

#[tokio::test]
async fn test_use_keyspace_case_sensitivity() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks_lower = unique_keyspace_name().to_lowercase();
    let ks_upper = ks_lower.to_uppercase();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS \"{}\" WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks_lower)).await.unwrap();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS \"{}\" WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks_upper)).await.unwrap();

    session
        .ddl(format!(
            "CREATE TABLE {}.tab (a text primary key)",
            ks_lower
        ))
        .await
        .unwrap();

    session
        .ddl(format!(
            "CREATE TABLE \"{}\".tab (a text primary key)",
            ks_upper
        ))
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO {}.tab (a) VALUES ('lowercase')", ks_lower),
            &[],
        )
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO \"{}\".tab (a) VALUES ('uppercase')", ks_upper),
            &[],
        )
        .await
        .unwrap();

    // Use uppercase keyspace without case sensitivity
    // Should select the lowercase one
    session.use_keyspace(ks_upper.clone(), false).await.unwrap();

    let rows: Vec<String> = session
        .query_unpaged("SELECT * from tab", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(String,)>()
        .unwrap()
        .map(|row| row.unwrap().0)
        .collect();

    assert_eq!(rows, vec!["lowercase".to_string()]);

    // Use uppercase keyspace with case sensitivity
    // Should select the uppercase one
    session.use_keyspace(ks_upper, true).await.unwrap();

    let rows: Vec<String> = session
        .query_unpaged("SELECT * from tab", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(String,)>()
        .unwrap()
        .map(|row| row.unwrap().0)
        .collect();

    assert_eq!(rows, vec!["uppercase".to_string()]);
}

#[tokio::test]
async fn test_raw_use_keyspace() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();

    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {}.tab (a text primary key)",
            ks
        ))
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO {}.tab (a) VALUES ('raw_test')", ks),
            &[],
        )
        .await
        .unwrap();

    session
        .query_unpaged(format!("use    \"{}\"    ;", ks), &[])
        .await
        .unwrap();

    let rows: Vec<String> = session
        .query_unpaged("SELECT * FROM tab", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(String,)>()
        .unwrap()
        .map(|res| res.unwrap().0)
        .collect();

    assert_eq!(rows, vec!["raw_test".to_string()]);

    // Check if case sensitivity is correctly detected
    assert!(session
        .query_unpaged(format!("use    \"{}\"    ;", ks.to_uppercase()), &[])
        .await
        .is_err());

    assert!(session
        .query_unpaged(format!("use    {}    ;", ks.to_uppercase()), &[])
        .await
        .is_ok());
}

#[tokio::test]
async fn test_get_keyspace_name() {
    let ks = unique_keyspace_name();

    // Create the keyspace
    // No keyspace is set in config, so get_keyspace() should return None.
    let session = create_new_session_builder().build().await.unwrap();
    assert_eq!(session.get_keyspace(), None);
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    assert_eq!(session.get_keyspace(), None);

    // Call use_keyspace(), get_keyspace now should return the new keyspace name
    session.use_keyspace(&ks, true).await.unwrap();
    assert_eq!(*session.get_keyspace().unwrap(), ks);

    // Creating a new session with the keyspace set in config should cause
    // get_keyspace to return that name
    let session = create_new_session_builder()
        .use_keyspace(&ks, true)
        .build()
        .await
        .unwrap();
    assert_eq!(*session.get_keyspace().unwrap(), ks);
}
