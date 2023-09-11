use std::{
    cell::RefCell,
    net::{IpAddr, SocketAddr},
};

use axum::{
    body::HttpBody,
    extract::ConnectInfo,
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use http::{
    header::{self, HeaderName},
    HeaderValue, Method, Request, StatusCode, Uri,
};
use hyper::Body;
use tokio::sync::{oneshot, Mutex};
use tracing::{info, instrument::{self, WithSubscriber}, span, warn, Level, info_span, field::{self, display}, Instrument, Span, Subscriber, Event};

use beam_lib::AppOrProxyId;
use tracing_subscriber::{fmt::{FormatEvent, FmtContext, format::{self, FmtSpan}, FormatFields}, registry::LookupSpan};

pub async fn log(
    req: Request<Body>,
    next: Next<Body>,
) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let span = info_span!("", from = field::Empty);

    async move {
        let resp = next.run(req).instrument(Span::current()).await;
        let status = resp.status();
        // If we get a gateway timeout we won't log it with log level warn as this happens regularly with the long polling api
        if resp.status().is_success() || resp.status().is_informational() || resp.status() == StatusCode::GATEWAY_TIMEOUT {
            info!(target: "in", "{method} {uri} {status}");
        } else {
            warn!(target: "in", "{method} {uri} {status}");
        };
        resp
    }.instrument(span).await
}
