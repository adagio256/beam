use std::{sync::Arc, collections::HashMap};

use axum::{Router, Json, extract::{State, Path}, routing::get, response::IntoResponse};
use hyper::{StatusCode, header};
use shared::{MsgSocketRequest, Encrypted, MsgSigned, HowLongToBlock, crypto_jwt::Authorized, MsgEmpty, Msg, MsgId, MsgSocketResult, HasWaitId};
use tokio::sync::{RwLock, broadcast::{Sender, self}, oneshot};
use tracing::{debug, log::error};

use crate::serve_tasks::{wait_for_elements_task, wait_get_statuscode};


#[derive(Clone)]
struct SocketState {
    socket_requests: Arc<RwLock<HashMap<MsgId, MsgSigned<MsgSocketRequest<Encrypted>>>>>,
    /// TODO: Figure out a better way to do this at some point
    new_socket_request: Arc<Sender<MsgSigned<MsgSocketRequest<Encrypted>>>>,
    deleted_socket_request: Arc<Sender<MsgId>>,
    new_result_tx: Arc<RwLock<HashMap<MsgId, Sender<MsgSigned<MsgSocketResult>>>>>,
}

impl Default for SocketState {
    fn default() -> Self {
        let (new_sender, _) = broadcast::channel(32);
        let (deleted_sender, _) = broadcast::channel(32);
        Self {
            socket_requests: Default::default(), 
            new_socket_request: Arc::new(new_sender),
            deleted_socket_request: Arc::new(deleted_sender),
            new_result_tx: Default::default()
        }
    }
}

pub(crate) fn router() -> Router {
    Router::new()
        .route("/v1/sockets", get(get_socket_requests).post(post_socket_request))
        .route("/v1/sockets/:id/results", get(get_socket_result).put(put_socket_result))
        .with_state(SocketState::default())
}


async fn get_socket_requests(
    block: HowLongToBlock,
    state: State<SocketState>,
    msg: MsgSigned<MsgEmpty>,
) -> (StatusCode, Json<Vec<MsgSigned<MsgSocketRequest<Encrypted>>>>) {
    let requester = msg.get_from();
    let filter = |req: &MsgSigned<MsgSocketRequest<Encrypted>>| &req.msg.to == requester;
    let mut socket_reqs = state.socket_requests
        .read()
        .await
        .values()
        .filter(|m| filter(*m))
        .cloned()
        .collect();

    wait_for_elements_task(
        &mut socket_reqs,
        &block,
        state.new_socket_request.subscribe(),
        filter,
        state.deleted_socket_request.subscribe()
    ).await;

    (wait_get_statuscode(&socket_reqs, &block), Json(socket_reqs))
}

async fn post_socket_request(
    state: State<SocketState>,
    msg: MsgSigned<MsgSocketRequest<Encrypted>>,
) -> Result<impl IntoResponse, impl IntoResponse> {
    let msg_id = msg.wait_id();
    {
        let mut task_lock = state.socket_requests.write().await;
        if task_lock.contains_key(&msg_id) {
            return Err((StatusCode::CONFLICT, format!("Msg Id {msg_id} is already taken.")));
        }
        task_lock.insert(msg_id.clone(), msg.clone());
    }

    if let Err(e) = state.new_socket_request.send(msg.clone()) {
        debug!("Unable to send notification: {}. Ignoring since probably noone is currently waiting for socket tasks.", e);
    }
    let (tx, _) = broadcast::channel(1);
    state.new_result_tx.write().await.insert(msg_id, tx);

    Ok((
        StatusCode::CREATED,
        [(header::LOCATION, format!("/v1/sockets/{}", msg_id))]
    ))
}

async fn put_socket_result(
    state: State<SocketState>,
    Path(task_id): Path<MsgId>,
    msg: MsgSigned<MsgSocketResult>
) -> (StatusCode, String) {
    if task_id != msg.msg.task {
        return (
            StatusCode::BAD_REQUEST,
            "Task IDs supplied in path and payload do not match.".to_string(),
        );
    };
    {
        let socket_req_map = &mut state.socket_requests.write().await;
        let Some(task) = socket_req_map.get_mut(&msg.msg.task) else {
            return (StatusCode::NOT_FOUND, "Socket task not found".to_string());
        };
        if msg.get_from() != &task.msg.to {
            return (StatusCode::UNAUTHORIZED, "Your result is not requested for this task.".to_string());
        }
        task.msg.result = Some(msg.msg.clone());
    }
    {
        let result_sender_map = &state.new_result_tx.read().await;
        let Some(tx) = result_sender_map.get(&msg.msg.task) else {
            error!("Found coresponding task but no sender was registerd for task {}.", msg.msg.task);
            return (StatusCode::INTERNAL_SERVER_ERROR, String::new());
        };
        if let Err(e) = tx.send(msg) {
            debug!("Unable to send notification: {}. Ignoring since probably noone is currently waiting for tasks.", e);
        };
    }
    (StatusCode::CREATED, "Successfully created result.".to_string())
}

async fn get_socket_result(
    state: State<SocketState>,
    msg: MsgSigned<MsgEmpty>
) {

}
