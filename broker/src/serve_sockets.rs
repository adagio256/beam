use std::{sync::Arc, collections::{HashMap, HashSet}, ops::Deref};

use axum::{Router, Json, extract::{State, Path}, routing::get, response::{IntoResponse, Response}};
use bytes::BufMut;
use hyper::{StatusCode, header, HeaderMap, http::HeaderValue, Body, Request, Method, upgrade::{OnUpgrade, self}};
use serde::{Serialize, Serializer, ser::SerializeSeq};
use shared::{MsgSocketRequest, Encrypted, MsgSigned, HowLongToBlock, crypto_jwt::Authorized, MsgEmpty, Msg, MsgId, HasWaitId, config::{CONFIG_SHARED, CONFIG_CENTRAL}};
use tokio::sync::{RwLock, broadcast::{Sender, self}, oneshot};
use tracing::{debug, log::error, warn};

use crate::{serve_tasks::wait_get_statuscode, task_manager::{TaskManager, Task}};


#[derive(Clone)]
struct SocketState {
    task_manager: Arc<TaskManager<MsgSocketRequest<Encrypted>>>,
    waiting_connections: Arc<RwLock<HashMap<MsgId, oneshot::Sender<Request<Body>>>>>
}

impl Default for SocketState {
    fn default() -> Self {
        Self {
            task_manager: TaskManager::new(),
            waiting_connections: Default::default()
        }
    }
}

pub(crate) fn router() -> Router {
    Router::new()
        .route("/v1/sockets", get(get_socket_requests).post(post_socket_request))
        .route("/v1/sockets/:id", get(connect_socket))
        .with_state(SocketState::default())
}

// Look into making a PR for Dashmap that makes its smartpointers serialize with the serde feature
fn serialize_deref_iter<S: Serializer>(serializer: S, iter: impl Iterator<Item = impl Deref<Target = impl Serialize>>) -> Result<S::Ok, S::Error> {
    let mut seq_ser = serializer.serialize_seq(iter.size_hint().1).map_err(serde::ser::Error::custom)?;
    for item in iter {
        seq_ser.serialize_element(item.deref())?;
    }
    seq_ser.end()
}

async fn get_socket_requests(
    block: HowLongToBlock,
    state: State<SocketState>,
    msg: MsgSigned<MsgEmpty>,
) -> Result<Response, StatusCode> {
    if block.wait_count.is_none() && block.wait_time.is_none() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let requester = msg.get_from();
    let filter = |req: &MsgSocketRequest<Encrypted>| req.to.contains(requester);

    let socket_reqs = state.task_manager.wait_for_tasks(&block, filter).await?;

    let writer = bytes::BytesMut::new().writer(); 
    let mut serializer = serde_json::Serializer::new(writer);
    if let Err(e) = serialize_deref_iter(&mut serializer, socket_reqs) {
        warn!("Failed to serialize socket tasks: {e}");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    Ok(Response::builder()
        .header(header::CONTENT_TYPE, HeaderValue::from_static("application/json"))
        .body(Body::from(serializer.into_inner().into_inner().freeze()))
        .expect("This is a proper Response")
        .into_response()
    )
}

async fn post_socket_request(
    state: State<SocketState>,
    msg: MsgSigned<MsgSocketRequest<Encrypted>>,
) -> Result<impl IntoResponse, StatusCode> {
    let msg_id = msg.wait_id();
    state.task_manager.post_task(msg)?;

    Ok((
        StatusCode::CREATED,
        [(header::LOCATION, format!("/v1/sockets/{}", msg_id))]
    ))
}

async fn connect_socket(
    state: State<SocketState>,
    task_id: MsgId,
    mut req: Request<Body>
    // This Result is just an Eiter type. An error value does not mean something went wrong
) -> Result<Response, StatusCode> {
    // We have to do this reconstruction of the request as calling extract on the req to get the body will take ownership of the request
    let (mut parts, body) = req.into_parts();
    let body = hyper::body::to_bytes(body)
        .await
        .ok()
        .and_then(|data| String::from_utf8(data.to_vec()).ok())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let result = shared::crypto_jwt::verify_with_extended_header::<MsgEmpty>(&mut parts, &body).await;
    let msg = match result {
        Ok(msg) => msg.msg,
        Err(e) => return Ok(e.into_response()),
    };
    {
        let task = state.task_manager.get(&task_id)?;
        // Allowed to connect are the issuer of the task and the recipient
        if !(task.get_from() == &msg.from || task.get_to().contains(&msg.from)) {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    req = Request::from_parts(parts, Body::empty());
    if req.extensions().get::<OnUpgrade>().is_none() {
        return Err(StatusCode::UPGRADE_REQUIRED);
    }

    let mut waiting_cons = state.waiting_connections.write().await;
    if let Some(req_sender) = waiting_cons.remove(&task_id) {
        if let Err(_) = req_sender.send(req) {
            warn!("Error sending socket connection to tunnel. Reciever has been dropped");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    } else {
        let (tx, rx) = tokio::sync::oneshot::channel();
        waiting_cons.insert(task_id, tx);
        // Drop write lock as we don't need it anymore
        drop(waiting_cons);
        let Ok(other_req) = rx.await else {
            debug!("Socket expired because nobody connected");
            return Err(StatusCode::GONE);
        };
        // We don't care if the task expired by now
        _ = state.task_manager.remove(&task_id);
        tokio::spawn(async move {
            let (mut socket1, mut socket2) = match tokio::try_join!(upgrade::on(req), upgrade::on(other_req)) {
                Ok(sockets) => sockets,
                Err(e) => {
                    warn!("Failed to upgrade requests to socket connections: {e}");
                    return;
                },
            };

            let result = tokio::io::copy_bidirectional(&mut socket1, &mut socket2).await;
            if let Err(e) = result {
                debug!("Relaying socket connection ended: {e}");
            }
        });
    }
    Err(StatusCode::SWITCHING_PROTOCOLS)
}
