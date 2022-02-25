use futures::stream::StreamExt;
use log::debug;
use rusoto_s3::S3;
use tokio::io::AsyncReadExt;
use uriparse::URIReference;

use crate::error::ColumnQError;
use crate::table::TableSource;

pub fn parse_uri<'a>(path: &'a str) -> Result<(&'a str, &'a str), ColumnQError> {
    let parts: Vec<&'a str> = path.split("://").collect();

    if parts.len() <= 1 || parts[0] != "s3" {
        return Err(ColumnQError::InvalidUri(format!(
            "{} is not a valid S3 URI",
            path
        )));
    }

    let mut path_parts = parts[1].splitn(2, '/');
    let bucket = path_parts
        .next()
        .ok_or_else(|| ColumnQError::InvalidUri("missing s3 bucket".to_string()))?;
    let key = path_parts
        .next()
        .ok_or_else(|| ColumnQError::InvalidUri("missing s3 key".to_string()))?;

    Ok((bucket, key))
}

fn new_s3_client() -> Result<rusoto_s3::S3Client, ColumnQError> {
    let region = rusoto_core::Region::default();
    let dispatcher = rusoto_core::HttpClient::new()
        .map_err(|_| ColumnQError::S3Store("Failed to create request dispatcher".to_string()))?;

    let client = match std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE") {
        Ok(_) => {
            let provider = rusoto_sts::WebIdentityProvider::from_k8s_env();
            let provider =
                rusoto_credential::AutoRefreshingProvider::new(provider).map_err(|e| {
                    ColumnQError::S3Store(format!(
                        "Failed to retrieve S3 credentials with message: {}",
                        e.message
                    ))
                })?;
            rusoto_s3::S3Client::new_with(dispatcher, provider, region)
        }
        Err(_) => rusoto_s3::S3Client::new_with(
            dispatcher,
            rusoto_core::credential::ChainProvider::new(),
            region,
        ),
    };

    Ok(client)
}

enum ContinuationToken {
    Value(Option<String>),
    End,
}

fn list_objects<'a>(
    bucket: &'a str,
    key: &'a str,
) -> Result<impl futures::Stream<Item = Result<String, ColumnQError>> + 'a, ColumnQError> {
    struct S3ListState<'a> {
        bucket: &'a str,
        key: String,
        client: rusoto_s3::S3Client,
        continuation_token: ContinuationToken,
        obj_iter: std::vec::IntoIter<rusoto_s3::Object>,
    }

    // add / suffix if missing to to bused in `start_after` listing to exlude directory itself from
    // the list result
    let key = if key.ends_with('/') {
        key.to_string()
    } else {
        let mut path = key.to_string();
        path.push('/');
        path
    };

    let init_state = S3ListState {
        bucket,
        key,
        client: new_s3_client()?,
        continuation_token: ContinuationToken::Value(None),
        obj_iter: Vec::new().into_iter(),
    };

    Ok(futures::stream::unfold(
        init_state,
        |mut state| async move {
            match state.obj_iter.next() {
                Some(obj) => Some((obj.key.ok_or_else(ColumnQError::s3_obj_missing_key), state)),
                None => match &state.continuation_token {
                    ContinuationToken::End => None, // terminate stream
                    ContinuationToken::Value(v) => {
                        let list_req = rusoto_s3::ListObjectsV2Request {
                            bucket: state.bucket.to_string(),
                            prefix: Some(state.key.clone()),
                            start_after: Some(state.key.clone()),
                            continuation_token: v.clone(),
                            ..Default::default()
                        };

                        let list_resp = match state.client.list_objects_v2(list_req).await {
                            Ok(res) => res,
                            Err(e) => {
                                return Some((
                                    Err(ColumnQError::S3Store(format!(
                                        "Failed to list bucket: {}",
                                        e
                                    ))),
                                    state,
                                ));
                            }
                        };
                        state.continuation_token = list_resp
                            .next_continuation_token
                            .map(|t| ContinuationToken::Value(Some(t)))
                            .unwrap_or(ContinuationToken::End);

                        state.obj_iter = list_resp.contents.unwrap_or_default().into_iter();
                        state.obj_iter.next().map(|obj| {
                            (obj.key.ok_or_else(ColumnQError::s3_obj_missing_key), state)
                        })
                    }
                },
            }
        },
    ))
}

pub async fn partition_key_to_reader(
    client: &rusoto_s3::S3Client,
    bucket: &str,
    partition_key: &str,
) -> Result<std::io::Cursor<Vec<u8>>, ColumnQError> {
    let get_req = rusoto_s3::GetObjectRequest {
        bucket: bucket.to_string(),
        key: partition_key.to_string(),
        ..Default::default()
    };

    let result = client.get_object(get_req).await.map_err(|e| {
        ColumnQError::S3Store(format!(
            "get_object error for (s3::{}/{}): {}",
            bucket, partition_key, e
        ))
    })?;

    let mut buf = Vec::new();
    let stream = result.body.ok_or_else(|| {
        ColumnQError::S3Store(format!(
            "missing object body data for s3://{}/{}",
            bucket, partition_key
        ))
    })?;
    stream
        .into_async_read()
        .read_to_end(&mut buf)
        .await
        .map_err(|e| {
            ColumnQError::S3Store(format!(
                "read object data error for (s3::{}/{}): {}",
                bucket, partition_key, e
            ))
        })?;

    Ok(std::io::Cursor::new(buf))
}

pub async fn partitions_from_path_iterator<'a, F, T, I>(
    path_iter: I,
    mut partition_reader: F,
) -> Result<Vec<T>, ColumnQError>
where
    I: Iterator<Item = &'a str>,
    F: FnMut(std::io::Cursor<Vec<u8>>) -> Result<T, ColumnQError>,
{
    let client = new_s3_client()?;
    let mut partitions = vec![];

    for s3_path in path_iter {
        let (bucket, key) = parse_uri(s3_path)?;
        let reader = partition_key_to_reader(&client, bucket, key).await?;
        partitions.push(partition_reader(reader)?);
    }

    Ok(partitions)
}

pub async fn partitions_from_uri<'a, F, T>(
    t: &'a TableSource,
    _uri: URIReference<'a>,
    mut partition_reader: F,
) -> Result<Vec<T>, ColumnQError>
where
    F: FnMut(std::io::Cursor<Vec<u8>>) -> Result<T, ColumnQError>,
{
    let region = if let Ok(endpoint_url) = std::env::var("AWS_ENDPOINT_URL") {
        debug!("using custom S3 endpoint: {}", &endpoint_url);
        rusoto_core::Region::Custom {
            name: "custom".to_string(),
            endpoint: endpoint_url,
        }
    } else {
        rusoto_core::Region::default()
    };
    let client = rusoto_s3::S3Client::new(region);
    // TODO: use host and path from URIReference instead
    let (bucket, key) = parse_uri(t.get_uri_str())?;

    let mut partitions = vec![];

    // first try loading table uri as single object
    match partition_key_to_reader(&client, bucket, key).await {
        Ok(reader) => {
            partitions.push(partition_reader(reader)?);
        }
        Err(_) => {
            // fallback to directory listing
            let mut key_stream = Box::pin(list_objects(bucket, key)?);
            // TODO: fetch s3 objects concurrently
            while let Some(item) = key_stream.next().await {
                let partition_key = item?;
                debug!("loading partition from: {}", partition_key);
                let reader = partition_key_to_reader(&client, bucket, &partition_key).await?;
                partitions.push(partition_reader(reader)?);
            }
        }
    }

    Ok(partitions)
}
