use std::sync::Arc;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use smol::prelude::*;

use crate::MelnetError;

/// An Endpoint asynchronously responds to Requests.
#[async_trait]
pub trait Endpoint<Req: DeserializeOwned + Send + 'static, Resp: Serialize>: Send + Sync {
    /// Handle a request. This should not block. Implementations should do things like move the Request to background tasks/threads to avoid this.
    async fn respond(&self, req: Request<Req>) -> anyhow::Result<Resp>;
}

#[async_trait]
impl<
        Req: DeserializeOwned + Send + 'static,
        Resp: Serialize,
        F: Fn(Request<Req>) -> R + 'static + Send + Sync,
        R: Future<Output = anyhow::Result<Resp>> + Send + 'static,
    > Endpoint<Req, Resp> for F
{
    async fn respond(&self, req: Request<Req>) -> anyhow::Result<Resp> {
        (self)(req).await
    }
}

/// Converts a responder to a boxed closure for internal use.
pub(crate) fn responder_to_closure<
    Req: DeserializeOwned + Send + 'static,
    Resp: Serialize + Send + 'static,
>(
    state: crate::NetState,
    responder: impl Endpoint<Req, Resp> + 'static + Send + Sync,
) -> BoxedResponder {
    let responder = Arc::new(responder);
    let clos = move |bts: &[u8]| {
        let decoded: Result<Req, _> = stdcode::deserialize(bts);
        let responder = responder.clone();
        let state = state.clone();
        match decoded {
            Ok(decoded) => {
                let response_fut = async move {
                    let response = responder
                        .respond(Request {
                            body: decoded,
                            state,
                        })
                        .await
                        .map_err(|e| MelnetError::Custom(e.to_string()))?;
                    Ok(stdcode::serialize(&response).unwrap())
                };
                response_fut.boxed()
            }
            Err(e) => {
                log::warn!("issue decoding request: {}", e);
                async { Err(MelnetError::InternalServerError) }.boxed()
            }
        }
    };
    BoxedResponder(Arc::new(clos))
}

#[allow(clippy::type_complexity)]
#[derive(Clone)]
pub(crate) struct BoxedResponder(
    pub Arc<dyn Fn(&[u8]) -> smol::future::Boxed<crate::Result<Vec<u8>>> + Send + Sync>,
);

/// A `Request<Req, Resp>` carries a stdcode-compatible request of type `Req and can be responded to with responses of type Resp.
#[must_use]
pub struct Request<Req: DeserializeOwned> {
    pub body: Req,
    pub state: crate::NetState,
}
