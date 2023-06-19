use std::{
    collections::HashMap, convert::Infallible, fmt::Debug, mem::Discriminant, net::SocketAddr,
    sync::Arc,
};

use axum::{
    extract::ConnectInfo,
    extract::{Path, Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{sse::Event, IntoResponse, Response, Sse},
    routing::{get, post, put},
    Json, Router,
};
use futures_core::{stream, Stream};
use hyper::HeaderMap;
use serde::Deserialize;
use shared::{
    beam_id::AppOrProxyId, config, errors::SamplyBeamError, sse_event::SseEventType,
    EncryptedMsgTaskRequest, EncryptedMsgTaskResult, HasWaitId, HowLongToBlock, Msg, MsgEmpty,
    MsgId, MsgSigned, MsgTaskRequest, MsgTaskResult, WorkStatus, EMPTY_VEC_APPORPROXYID,
};
use tokio::{
    sync::{
        broadcast::{Receiver, Sender},
        RwLock,
    },
    time,
};
use tracing::{debug, error, info, trace, warn};

use crate::expire;

#[derive(Clone)]
struct TasksState {
    tasks: Arc<RwLock<HashMap<MsgId, MsgSigned<EncryptedMsgTaskRequest>>>>,
    new_task_tx: Arc<Sender<MsgSigned<EncryptedMsgTaskRequest>>>,
    new_result_tx: Arc<RwLock<HashMap<MsgId, Sender<MsgSigned<EncryptedMsgTaskResult>>>>>,
    removed_task_rx: Arc<Sender<MsgId>>,
}

pub(crate) fn router() -> Router {
    let state = TasksState::default();
    let state2 = state.clone();
    tokio::task::spawn(async move {
        let err = expire::watch(state2.tasks.clone(), state2.new_task_tx.subscribe()).await;
        error!("Internal error: expire() returned with error {:?}", err);
    });
    Router::new()
        .route("/v1/tasks", get(get_tasks).post(post_task))
        .route("/v1/tasks/:task_id/results", get(get_results_for_task))
        .route("/v1/tasks/:task_id/results/:app_id", put(put_result))
        .with_state(state)
}

impl Default for TasksState {
    fn default() -> Self {
        let tasks: HashMap<MsgId, MsgSigned<EncryptedMsgTaskRequest>> = HashMap::new();
        let (new_task_tx, _) =
            tokio::sync::broadcast::channel::<MsgSigned<EncryptedMsgTaskRequest>>(512);

        let tasks = Arc::new(RwLock::new(tasks));
        let new_task_tx = Arc::new(new_task_tx);
        TasksState {
            tasks,
            new_task_tx,
            new_result_tx: Arc::new(RwLock::new(HashMap::new())),
            removed_task_rx: Arc::new(tokio::sync::broadcast::channel(512).0),
        }
    }
}

async fn get_results_for_task(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<TasksState>,
    block: HowLongToBlock,
    task_id: MsgId,
    headers: HeaderMap,
    msg: MsgSigned<MsgEmpty>,
) -> Result<Response, (StatusCode, &'static str)> {
    let found = &headers
        .get(header::ACCEPT)
        .unwrap_or(&HeaderValue::from_static(""))
        .to_str()
        .unwrap_or_default()
        .split(',')
        .map(|part| part.trim())
        .find(|part| *part == "text/event-stream")
        .is_some();

    let result = if *found {
        get_results_for_task_stream(addr, state, block, task_id, msg)
            .await?
            .into_response()
    } else {
        get_results_for_task_nostream(addr, state, block, task_id, msg)
            .await?
            .into_response()
    };
    Ok(result)
}

// GET /v1/tasks/:task_id/results
async fn get_results_for_task_nostream(
    addr: SocketAddr,
    state: TasksState,
    block: HowLongToBlock,
    task_id: MsgId,
    msg: MsgSigned<MsgEmpty>,
) -> Result<(StatusCode, Json<Vec<MsgSigned<EncryptedMsgTaskResult>>>), (StatusCode, &'static str)>
{
    debug!(
        "get_results_for_task(task={}) called by {} with IP {addr}, wait={:?}",
        task_id.to_string(),
        msg.get_from(),
        block
    );
    let filter_for_me = MsgFilterNoTask {
        from: None,
        to: Some(msg.get_from()),
        mode: MsgFilterMode::Or,
    };
    let (mut results, rx_new_result, rx_deleted_task) = {
        let tasks = state.tasks.read().await;
        let Some(task) = tasks.get(&task_id) else {
            return Err((StatusCode::NOT_FOUND, "Task not found"));
        };
        if task.get_from() != msg.get_from() {
            return Err((StatusCode::UNAUTHORIZED, "Not your task."));
        }
        let results: Vec<MsgSigned<EncryptedMsgTaskResult>> =
            task.msg.results.values().cloned().collect();
        let rx_new_result = match would_wait_for_elements(results.len(), &block) {
            true => Some(
                state
                    .new_result_tx
                    .read()
                    .await
                    .get(&task_id)
                    .unwrap_or_else(|| {
                        panic!(
                            "Internal error: No new_result_tx found for task {}",
                            task_id
                        )
                    })
                    .subscribe(),
            ),
            false => None,
        };
        (results, rx_new_result, state.removed_task_rx.subscribe())
    };
    if let Some(rx) = rx_new_result {
        wait_for_results_for_task(
            &mut results,
            &block,
            rx,
            move |m| filter_for_me.matches(m),
            rx_deleted_task,
            &task_id,
        )
        .await;
    }
    let statuscode = wait_get_statuscode(&results, &block);
    Ok((statuscode, Json(results)))
}

// GET /v1/tasks/:task_id/results/stream
async fn get_results_for_task_stream(
    addr: SocketAddr,
    state: TasksState,
    block: HowLongToBlock,
    task_id: MsgId,
    msg: MsgSigned<MsgEmpty>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, &'static str)> {
    debug!(
        "get_results_for_task_stream(task={}) called by {} with IP {addr}, wait={:?}",
        task_id.to_string(),
        msg.get_from(),
        block
    );
    let (mut results, rx_new_result, rx_deleted_task) = {
        let tasks = state.tasks.read().await;
        let Some(task) = tasks.get(&task_id) else {
            return Err((StatusCode::NOT_FOUND, "Task not found"));
        };
        if task.get_from() != msg.get_from() {
            return Err((StatusCode::UNAUTHORIZED, "Not your task."));
        }
        let results = task.msg.results.clone();
        let rx_new_result = match would_wait_for_elements(results.len(), &block) {
            true => Some(
                state
                    .new_result_tx
                    .read()
                    .await
                    .get(&task_id)
                    .unwrap_or_else(|| {
                        panic!(
                            "Internal error: No new_result_tx found for task {}",
                            task_id
                        )
                    })
                    .subscribe(),
            ),
            false => None,
        };
        (results, rx_new_result, state.removed_task_rx.subscribe())
    };

    let stream = async_stream::stream! {
        for (_from, result) in &results {
            let event = Event::default()
                .event(SseEventType::NewResult)
                .json_data(result);
            yield match event {
                Ok(event) => Ok(event),
                Err(err) => {
                    error!("Unable to serialize message: {}; offending message was {:?}", err, result);
                    Ok(Event::default()
                        .event(SseEventType::Error)
                        .data("Internal error: Unable to serialize message.")
                    )
                }
            };
        }
        if let Some(rx_new_result) = rx_new_result {
            let from = msg.get_from();
            let filter_for_me = MsgFilterNoTask { from: None, to: Some(&from), mode: MsgFilterMode::Or };
            let other_stream = wait_for_results_for_task_stream(&mut results, &block, rx_new_result, &filter_for_me, rx_deleted_task, &task_id).await;
            for await event in other_stream {
                yield event;
            }
        }
    };

    let sse = Sse::new(stream);

    Ok(sse)
}

fn would_wait_for_elements(existing_elements: usize, block: &HowLongToBlock) -> bool {
    usize::from(block.wait_count.unwrap_or(0)) > existing_elements
}

pub(crate) fn wait_get_statuscode<S>(vec: &Vec<S>, block: &HowLongToBlock) -> StatusCode {
    if usize::from(block.wait_count.unwrap_or(0)) > vec.len() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    }
}

async fn wait_for_results_for_task_stream<'a, M: Msg, I: PartialEq>(
    results: &'a mut HashMap<AppOrProxyId, M>,
    block: &'a HowLongToBlock,
    mut new_result_rx: Receiver<M>,
    filter: &'a MsgFilterNoTask<'a>,
    mut deleted_task_rx: Receiver<MsgId>,
    task_id: &'a MsgId,
) -> impl Stream<Item = Result<Event, Infallible>> + 'a
where
    M: Clone + HasWaitId<I> + Debug,
{
    let wait_until = time::Instant::now()
        + block
            .wait_time
            .unwrap_or(time::Duration::from_secs(31536000));
    trace!(
        "Now is {:?}. Will wait until {:?}",
        time::Instant::now(),
        wait_until
    );
    let mut running = true;
    let stream = async_stream::stream! {
    while usize::from(block.wait_count.unwrap_or(0)) > results.len()
        && time::Instant::now() < wait_until
        && running {
        trace!(
            "Items in vec: {}, time remaining: {:?}",
            results.len(),
            wait_until - time::Instant::now()
        );
            tokio::select! {
                _ = tokio::time::sleep_until(wait_until) => {
                    debug!("SSE: Wait expired.");
                    yield Ok(Event::default()
                        .event(SseEventType::WaitExpired)
                        .data("{}"));
                    running = false;
                },
                result = new_result_rx.recv() => {
                    match result {
                        Ok(req) => {
                            if filter.matches(&req) {
                                let previous = results.insert(req.get_from().clone(), req.clone());
                                let event_type = match previous {
                                    Some(_) => SseEventType::UpdatedResult,
                                    None => SseEventType::NewResult
                                };
                                let event = Event::default()
                                    .event(event_type)
                                    .json_data(&req);
                                yield match event {
                                    Ok(event) => Ok(event),
                                    Err(err) => {
                                        error!("Unable to serialize message: {}; offending message was {:?}", err, req);
                                        Ok(Event::default()
                                            .event(SseEventType::Error)
                                            .data("Internal error: Unable to serialize message.")
                                        )
                                    }
                                };
                            }
                        },
                        Err(e) => { panic!("Unable to receive from queue new_result_rx: {}", e); }
                    }
                },
                deleted_task_id = deleted_task_rx.recv() => {
                    match deleted_task_id {
                        Ok(deleted_task_id) => {
                            if deleted_task_id == *task_id {
                                warn!("Task {} was just deleted while someone was waiting for results. Returning the {} results up to now.", task_id, results.len());
                                yield Ok(Event::default()
                                    .event(SseEventType::DeletedTask)
                                    .data("{ \"task_id\": \"{deleted_task_id}\" }"));
                                running = false;
                            }
                        },
                        Err(e) => { panic!("Unable to receive from queue deleted_task_rx: {}", e); }
                    }
                }
            }
        }
    };
    stream
}

// TODO: Is there a way to write this function in a generic way? (1/2)
pub(crate) async fn wait_for_results_for_task<'a, M: Msg, I: PartialEq>(
    vec: &mut Vec<M>,
    block: &HowLongToBlock,
    mut new_result_rx: Receiver<M>,
    filter: impl Fn(&M) -> bool,
    mut deleted_task_rx: Receiver<MsgId>,
    task_id: &MsgId,
) where
    M: Clone + HasWaitId<I>,
{
    let wait_until = time::Instant::now()
        + block
            .wait_time
            .unwrap_or(time::Duration::from_secs(31536000));
    trace!(
        "Now is {:?}. Will wait until {:?}",
        time::Instant::now(),
        wait_until
    );
    while usize::from(block.wait_count.unwrap_or(0)) > vec.len()
        && time::Instant::now() < wait_until
    {
        trace!(
            "Items in vec: {}, time remaining: {:?}",
            vec.len(),
            wait_until - time::Instant::now()
        );
        tokio::select! {
            _ = tokio::time::sleep_until(wait_until) => {
                break;
            },
            result = new_result_rx.recv() => {
                match result {
                    Ok(req) => {
                        if filter(&req) {
                            vec.retain(|el| el.wait_id() != req.wait_id());
                            vec.push(req);
                        }
                    },
                    Err(e) => { panic!("Unable to receive from queue new_result_rx: {}", e); }
                }
            },
            deleted_task_id = deleted_task_rx.recv() => {
                match deleted_task_id {
                    Ok(deleted_task_id) => {
                        if deleted_task_id == *task_id {
                            warn!("Task {} was just deleted while someone was waiting for results. Returning the {} results up to now.", task_id, vec.len());
                            return;
                        }
                    },
                    Err(e) => { panic!("Unable to receive from queue deleted_task_rx: {}", e); }
                }
            }
        }
    }
}

pub(crate) async fn wait_for_elements_task<M: HasWaitId<MsgId> + Clone>(
    vec: &mut Vec<M>,
    block: &HowLongToBlock,
    mut new_element_rx: Receiver<M>,
    filter: impl Fn(&M) -> bool,
    mut deleted_task_rx: Receiver<MsgId>,
) {
    let wait_until = time::Instant::now()
        + block
            .wait_time
            .unwrap_or(time::Duration::from_secs(31536000));
    trace!(
        "Now is {:?}. Will wait until {:?}",
        time::Instant::now(),
        wait_until
    );
    while usize::from(block.wait_count.unwrap_or(0)) > vec.len()
        && time::Instant::now() < wait_until
    {
        trace!(
            "Items in vec: {}, time remaining: {:?}",
            vec.len(),
            wait_until - time::Instant::now()
        );
        tokio::select! {
            _ = tokio::time::sleep_until(wait_until) => {
                break;
            },
            result = new_element_rx.recv() => {
                match result {
                    Ok(req) => {
                        if filter(&req) {
                            vec.retain(|el| el.wait_id() != req.wait_id());
                            vec.push(req);
                        }
                    },
                    Err(_) => { panic!("Unable to receive from queue! What happened?"); }
                }
            },
            deleted_task_id = deleted_task_rx.recv() => {
                match deleted_task_id {
                    Ok(deleted_task_id) => {
                        vec.retain(|el| el.wait_id() != deleted_task_id);
                    },
                    Err(_) => { panic!("Unable to receive from queue deleted_task_rx! What happened?"); }
                }
            }
        }
    }
}

#[derive(Deserialize)]
struct TaskFilter {
    from: Option<AppOrProxyId>,
    to: Option<AppOrProxyId>,
    filter: Option<FilterParam>,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum FilterParam {
    Todo,
}

/// GET /v1/tasks
/// Will retrieve tasks that are at least FROM or TO the supplied parameters.
async fn get_tasks(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    block: HowLongToBlock,
    Query(taskfilter): Query<TaskFilter>,
    State(state): State<TasksState>,
    msg: MsgSigned<MsgEmpty>,
) -> Result<(StatusCode, impl IntoResponse), (StatusCode, impl IntoResponse)> {
    let from = taskfilter.from;
    let mut to = taskfilter.to;
    let unanswered_by = match taskfilter.filter {
        Some(FilterParam::Todo) => {
            if to.is_none() {
                to = Some(msg.get_from().clone());
            }
            Some(msg.get_from().clone())
        }
        None => None,
    };
    if from.is_none() && to.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Please supply either \"from\" or \"to\" query parameter.",
        ));
    }
    if (from.is_some() && *from.as_ref().unwrap() != msg.msg.from)
        || (to.is_some() && *to.as_ref().unwrap() != msg.msg.from)
    {
        // Rewrite in Rust 1.64: https://github.com/rust-lang/rust/pull/94927
        return Err((
            StatusCode::UNAUTHORIZED,
            "You can only list messages created by you (from) or directed to you (to).",
        ));
    }
    // Step 1: Get initial vector fill from HashMap + receiver for new elements
    let filter = MsgFilterNoTask {
        from: from.as_ref(),
        to: to.as_ref(),
        mode: MsgFilterMode::Or,
    };
    let filter = MsgFilterForTask {
        normal: filter,
        unanswered_by: unanswered_by.as_ref(),
        workstatus_is_not: [WorkStatus::Succeeded, WorkStatus::PermFailed]
            .iter()
            .map(std::mem::discriminant)
            .collect(),
    };
    let (mut vec, new_task_rx) = {
        let map = state.tasks.read().await;
        let vec: Vec<MsgSigned<EncryptedMsgTaskRequest>> = map
            .iter()
            .filter_map(|(_, v)| {
                if filter.matches(v) {
                    Some(v.clone())
                } else {
                    None
                }
            })
            .collect();
        (vec, state.new_task_tx.subscribe())
    };
    // Step 2: Extend vector with new elements, waiting for `block` amount of time/items
    wait_for_elements_task(
        &mut vec,
        &block,
        new_task_rx,
        move |m| filter.matches(m),
        state.removed_task_rx.subscribe(),
    )
    .await;
    let statuscode = wait_get_statuscode(&vec, &block);
    Ok((statuscode, Json(vec)))
}

trait MsgFilterTrait<M: Msg> {
    // fn new() -> Self;
    fn from(&self) -> Option<&AppOrProxyId>;
    fn to(&self) -> Option<&AppOrProxyId>;
    fn mode(&self) -> &MsgFilterMode;

    fn matches(&self, msg: &M) -> bool {
        match self.mode() {
            MsgFilterMode::Or => self.filter_or(msg),
            MsgFilterMode::And => self.filter_and(msg),
        }
    }

    /// Returns true iff the from or the to conditions match (or both)
    fn filter_or(&self, msg: &M) -> bool {
        if self.from().is_none() && self.to().is_none() {
            return true;
        }
        if let Some(to) = &self.to() {
            if msg.get_to().contains(to) {
                return true;
            }
        }
        if let Some(from) = &self.from() {
            if msg.get_from() == *from {
                return true;
            }
        }
        false
    }

    /// Returns true iff all defined from/to conditions are met.
    fn filter_and(&self, msg: &M) -> bool {
        if self.from().is_none() && self.to().is_none() {
            return true;
        }
        if let Some(to) = self.to() {
            if !msg.get_to().contains(to) {
                return false;
            }
        }
        if let Some(from) = &self.from() {
            if msg.get_from() != *from {
                return false;
            }
        }
        true
    }
}

#[allow(dead_code)]
enum MsgFilterMode {
    Or,
    And,
}
struct MsgFilterNoTask<'a> {
    from: Option<&'a AppOrProxyId>,
    to: Option<&'a AppOrProxyId>,
    mode: MsgFilterMode,
}

struct MsgFilterForTask<'a> {
    normal: MsgFilterNoTask<'a>,
    unanswered_by: Option<&'a AppOrProxyId>,
    workstatus_is_not: Vec<Discriminant<WorkStatus>>,
}

impl<'a> MsgFilterForTask<'a> {
    fn unanswered(&self, msg: &EncryptedMsgTaskRequest) -> bool {
        if self.unanswered_by.is_none() {
            debug!("Is {} unanswered? Yes, criterion not defined.", msg.id());
            return true;
        }
        let unanswered = self.unanswered_by.unwrap();
        for res in msg.results.values() {
            if res.get_from() == unanswered
                && self
                    .workstatus_is_not
                    .contains(&std::mem::discriminant(&res.msg.status))
            {
                debug!("Is {} unanswered? No, answer found.", msg.id());
                return false;
            }
        }
        debug!("Is {} unanswered? Yes, no matching answer found.", msg.id());
        true
    }
}

impl<'a> MsgFilterTrait<MsgSigned<EncryptedMsgTaskRequest>> for MsgFilterForTask<'a> {
    fn from(&self) -> Option<&AppOrProxyId> {
        self.normal.from
    }

    fn to(&self) -> Option<&AppOrProxyId> {
        self.normal.to
    }

    fn matches(&self, msg: &MsgSigned<EncryptedMsgTaskRequest>) -> bool {
        MsgFilterNoTask::matches(&self.normal, msg) && self.unanswered(&msg.msg)
    }

    fn mode(&self) -> &MsgFilterMode {
        &self.normal.mode
    }
}

impl<'a, M: Msg> MsgFilterTrait<M> for MsgFilterNoTask<'a> {
    fn from(&self) -> Option<&AppOrProxyId> {
        self.from
    }

    fn to(&self) -> Option<&AppOrProxyId> {
        self.to
    }

    fn mode(&self) -> &MsgFilterMode {
        &self.mode
    }
}

// POST /v1/tasks
async fn post_task(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<TasksState>,
    msg: MsgSigned<EncryptedMsgTaskRequest>,
) -> Result<(StatusCode, impl IntoResponse), (StatusCode, String)> {
    // let id = MsgId::new();
    // msg.id = id;
    // TODO: Check if ID is taken
    trace!(
        "Client {} with IP {addr} is creating task {:?}",
        msg.msg.from, msg
    );
    let (new_tx, _) = tokio::sync::broadcast::channel(256);
    {
        let mut tasks = state.tasks.write().await;
        let mut txes = state.new_result_tx.write().await;
        if tasks.contains_key(&msg.msg.id) {
            return Err((
                StatusCode::CONFLICT,
                format!("ID {} is already taken.", msg.msg.id),
            ));
        }
        tasks.insert(msg.msg.id, msg.clone());
        txes.insert(msg.msg.id, new_tx);
        if let Err(e) = state.new_task_tx.send(msg.clone()) {
            debug!("Unable to send notification: {}. Ignoring since probably noone is currently waiting for tasks.", e);
        }
    }
    Ok((
        StatusCode::CREATED,
        [(header::LOCATION, format!("/v1/tasks/{}", msg.msg.id))],
    ))
}

// PUT /v1/tasks/:task_id/results/:app_id
async fn put_result(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path((task_id, app_id)): Path<(MsgId, AppOrProxyId)>,
    State(state): State<TasksState>,
    result: MsgSigned<EncryptedMsgTaskResult>,
) -> Result<StatusCode, (StatusCode, &'static str)> {
    trace!("Called: Task {:?}, {:?} by {addr}", task_id, result);
    if task_id != result.msg.task {
        return Err((
            StatusCode::BAD_REQUEST,
            "Task IDs supplied in path and payload do not match.",
        ));
    }
    let worker_id = result.msg.from.clone();
    if app_id != worker_id {
        return Err((
            StatusCode::BAD_REQUEST,
            "AppID supplied in URL and signed message do not match.",
        ));
    }

    // Step 1: Check prereqs.
    let mut tasks = state.tasks.write().await;

    // TODO: Check if this can be written nicer using .entry()
    let task = match tasks.get_mut(&task_id) {
        Some(task) => &mut task.msg,
        None => return Err((StatusCode::NOT_FOUND, "Task not found")),
    };
    trace!(?task, ?worker_id, "Checking if task is in worker ID: ");
    if !task.to.contains(&worker_id) {
        return Err((
            StatusCode::UNAUTHORIZED,
            "Your result is not requested for this task.",
        ));
    }

    // Step 2: Insert.
    let statuscode = match task.results.insert(worker_id.clone(), result.clone()) {
        Some(_) => StatusCode::NO_CONTENT,
        None => StatusCode::CREATED,
    };

    // Step 3: Notify. This has to happen while the lock for tasks is still held since otherwise results could get lost.
    let sender = state.new_result_tx.read().await;
    let sender = sender
        .get(&task_id)
        .unwrap_or_else(|| panic!("Internal error: No result_tx found for task {}", task_id));
    if let Err(e) = sender.send(result) {
        debug!("Unable to send notification: {}. Ignoring since probably noone is currently waiting for tasks.", e);
    }
    Ok(statuscode)
}

#[cfg(all(test, never))] // Removed until the errors down below are fixed
mod test {
    use serde_json::Value;
    use shared::{
        beam_id::{AppId, AppOrProxyId, BeamId, BrokerId, ProxyId},
        EncryptedMsgTaskRequest, Msg, MsgSigned, MsgTaskRequest, MsgTaskResult, WorkStatus,
    };

    use super::{MsgFilterForTask, MsgFilterMode, MsgFilterNoTask, MsgFilterTrait};

    #[test]
    fn filter_task() {
        const BROKER_ID: &str = "broker";
        BrokerId::set_broker_id(BROKER_ID.into());
        let broker = BrokerId::new(BROKER_ID).unwrap();
        let proxy = ProxyId::random(&broker);
        let app1: AppOrProxyId = AppId::new(&format!("app1.{}", proxy)).unwrap().into();
        let app2: AppOrProxyId = AppId::new(&format!("app2.{}", proxy)).unwrap().into();
        let task = MsgTaskRequest::new(
            app1.clone(),
            vec![app2.clone()],
            "Important task".into(),
            shared::FailureStrategy::Retry {
                backoff_millisecs: 1000,
                max_tries: 5,
            },
            Value::Null,
        );
        let result_by_app2 = MsgTaskResult {
            from: app2.clone(),
            to: vec![task.get_from().clone()],
            task: *task.id(),
            status: WorkStatus::TempFailed,
            metadata: Value::Null,
            body: Some("I'd like to retry, please re-send this task".into()),
        };
        let result_by_app2 = MsgSigned {
            msg: result_by_app2,
            sig: "Certainly valid".into(),
        };
        let mut task = MsgSigned {
            msg: task,
            sig: "Certainly valid".into(),
        };
        // let a = app1.clone();
        let filter = MsgFilterNoTask {
            from: None,
            to: Some(&app2),
            mode: MsgFilterMode::Or,
        };
        let filter = MsgFilterForTask {
            normal: filter,
            unanswered_by: Some(&app2),
            workstatus_is_not: [WorkStatus::Succeeded, WorkStatus::PermFailed]
                .iter()
                .map(std::mem::discriminant)
                .collect(),
        };
        assert_eq!(
            filter.matches(&task),
            true,
            "There are no results yet, so I should get the task: {:?}",
            task
        );
        task.msg
            .results
            .insert(result_by_app2.get_from().clone(), result_by_app2);
        assert_eq!(
            filter.matches(&task),
            true,
            "The only result is TempFailed, so I should still get it: {:?}",
            task
        );

        let result_by_app2 = task.msg.results.get_mut(&app2).unwrap();
        result_by_app2.msg.status = WorkStatus::Succeeded;
        assert_eq!(
            filter.matches(&task),
            false,
            "It's done, so I shouldn't get it"
        );
    }
}
