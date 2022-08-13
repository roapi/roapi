use axum::http::uri::Uri;
use axum::http::Method;
use axum::http::Request;
use axum::http::Response;
use hyper::service::Service;
use log::error;
use log::info;
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;
use tower_layer::Layer;

pub struct HttpLoggerLayer {}

impl HttpLoggerLayer {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for HttpLoggerLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for HttpLoggerLayer {
    type Service = HttpLogger<S>;

    fn layer(&self, service: S) -> Self::Service {
        HttpLogger::new(service)
    }
}

#[derive(Debug, Clone)]
pub struct HttpLogger<Inner> {
    inner: Inner,
}

impl<Inner> HttpLogger<Inner> {
    fn new(inner: Inner) -> Self {
        Self { inner }
    }
}

impl<Inner, ReqBody, ResBody> Service<Request<ReqBody>> for HttpLogger<Inner>
where
    Inner: Service<Request<ReqBody>, Response = Response<ResBody>>,
    Inner::Error: std::fmt::Debug,
{
    type Response = Inner::Response;
    type Error = Inner::Error;
    type Future = LoggerResponseFuture<Inner::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        // TODO: user-agent can be extracted from request.headers()
        let method = request.method().to_owned();
        let uri = request.uri().to_owned();
        let response_future = self.inner.call(request);
        LoggerResponseFuture {
            response_future,
            method,
            uri,
            start: None,
        }
    }
}

pin_project! {
    pub struct LoggerResponseFuture<F> {
        #[pin]
        response_future: F,
        method: Method,
        uri: Uri,
        start: Option<Instant>,
    }
}

impl<F, Body, E> Future for LoggerResponseFuture<F>
where
    F: Future<Output = Result<Response<Body>, E>>,
    E: std::fmt::Debug,
{
    type Output = Result<Response<Body>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let start = this.start.get_or_insert_with(Instant::now);

        match this.response_future.poll(cx) {
            Poll::Ready(result) => {
                match &result {
                    Ok(resp) => {
                        let elapsed = start.elapsed();
                        info!(
                            "[{}] {:?} {} {:?}",
                            resp.status(),
                            this.method,
                            this.uri,
                            elapsed,
                        );
                    }
                    Err(err) => {
                        error!("{:?} {}: {:?}", this.method, this.uri, err);
                    }
                }
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
