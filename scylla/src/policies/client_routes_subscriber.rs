use std::sync::Arc;

use async_trait::async_trait;

use crate::cluster::metadata::ClientRoutes;

#[expect(unused)] // temporary, removed in a further commit
#[async_trait]
pub(crate) trait ClientRoutesSubscriber {
    async fn update_client_routes(&self, client_routes: &Arc<ClientRoutes>);
}
