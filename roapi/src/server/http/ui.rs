use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "../roapi-ui/dist"]
struct Asset;

pub struct StaticFile<T>(pub T);

impl<T> IntoResponse for StaticFile<T>
where
    T: Into<String>,
{
    fn into_response(self) -> Response {
        let path = self.0.into();

        match Asset::get(path.as_str()) {
            Some(content) => {
                let mime = mime_guess::from_path(path).first_or_octet_stream();
                ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response()
            }
            None => (StatusCode::NOT_FOUND, "404 Not Found").into_response(),
        }
    }
}

async fn ui_index_handler() -> impl IntoResponse {
    StaticFile("index.html")
}

async fn static_handler(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches("/").to_string();
    StaticFile(path)
}

pub fn ui_router() -> axum::Router {
    axum::Router::new()
        .route("/", get(ui_index_handler))
        .route("/index.html", get(ui_index_handler))
        .route("/*path", get(static_handler))
}
