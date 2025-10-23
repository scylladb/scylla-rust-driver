use tracing::debug;

use crate::utils::{create_new_session_builder, setup_tracing};

#[tokio::test]
#[ntest::timeout(1000)]
async fn test_basic() {
    setup_tracing();
    debug!("Test");
    let session = create_new_session_builder().build().await.unwrap();
    let prepared = session
        .prepare("SELECT host_id FROM system.local WHERE key='local'")
        .await
        .unwrap();
    session.execute_iter(prepared, &[]).await.unwrap();
}
