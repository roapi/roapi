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
    let bucket = match path_parts.next() {
        Some(x) => x,
        None => {
            return Err(ColumnQError::InvalidUri("missing s3 bucket".to_string()));
        }
    };
    let key = match path_parts.next() {
        Some(x) => x,
        None => {
            return Err(ColumnQError::InvalidUri("missing s3 key".to_string()));
        }
    };

    Ok((bucket, key))
}

enum ContinuationToken {
    Value(Option<String>),
    End,
}

fn list_objects<'a>(
    bucket: &'a str,
    key: &'a str,
) -> impl futures::Stream<Item = Result<String, ColumnQError>> + 'a {
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
        client: rusoto_s3::S3Client::new(rusoto_core::Region::default()),
        continuation_token: ContinuationToken::Value(None),
        obj_iter: Vec::new().into_iter(),
    };

    futures::stream::unfold(init_state, |mut state| async move {
        match state.obj_iter.next() {
            Some(obj) => Some((Ok(obj.key.unwrap()), state)),
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
                    state.obj_iter = match list_resp.contents {
                        Some(objs) => objs.into_iter(),
                        None => Vec::new().into_iter(),
                    };

                    state
                        .obj_iter
                        .next()
                        .map(|obj| (Ok(obj.key.unwrap()), state))
                }
            },
        }
    })
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

    let result = client
        .get_object(get_req)
        .await
        .map_err(|e| ColumnQError::S3Store(format!("get_object error: {}", e)))?;

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
        .map_err(|e| ColumnQError::S3Store(format!("read object data error: {}", e)))?;

    Ok(std::io::Cursor::new(buf))
}

pub async fn partitions_from_uri<'a, F, T>(
    t: &'a TableSource,
    _uri: URIReference<'a>,
    mut partition_reader: F,
) -> Result<Vec<T>, ColumnQError>
where
    F: FnMut(std::io::Cursor<Vec<u8>>) -> Result<T, ColumnQError>,
{
    let client = rusoto_s3::S3Client::new(rusoto_core::Region::default());
    let (bucket, key) = parse_uri(&t.uri)?;

    let mut partitions = vec![];

    // first try loading table uri as single object
    match partition_key_to_reader(&client, bucket, &key).await {
        Ok(reader) => {
            partitions.push(partition_reader(reader)?);
        }
        Err(_) => {
            // fallback to directory listing
            let mut key_stream = Box::pin(list_objects(bucket, key));
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
