use std::sync::Arc;

use async_trait::async_trait;

use crate::cluster::metadata::ClientRoutes;

#[async_trait]
pub(crate) trait ClientRoutesSubscriber {
    async fn update_client_routes(&self, client_routes: &Arc<ClientRoutes>);
}
