use crate::table::TableSource;
use futures::TryStreamExt;
use log::debug;
use object_store::ObjectStore;
use percent_encoding;
use snafu::prelude::*;
use std::str::FromStr;
use std::sync::Arc;
use uriparse::URIReference;
use url::Url;

use crate::io;
use crate::table;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Could not resolve store for url: {url}"))]
    GetStore {
        source: datafusion::error::DataFusionError,
        url: Url,
    },
    #[snafu(display("Could not get object: {path}"))]
    GetObject {
        source: object_store::Error,
        path: object_store::path::Path,
    },
    #[snafu(display("Could not list object: {path}"))]
    ListObject {
        source: object_store::Error,
        path: object_store::path::Path,
    },
    #[snafu(display("Could not read object bytes"))]
    ReadObjectBytes { source: object_store::Error },
    #[snafu(display("Could not load table data"))]
    Table { source: table::Error },
}

impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        Self::Generic {
            backend: "object_store",
            source: Box::new(err),
        }
    }
}

pub async fn partition_key_to_reader(
    client: Arc<dyn ObjectStore>,
    path: &object_store::path::Path,
) -> Result<std::io::Cursor<Vec<u8>>, Error> {
    let get_result = client.get(path).await.context(GetObjectSnafu {
        path: path.to_owned(),
    })?;
    let bytes = get_result.bytes().await.context(ReadObjectBytesSnafu)?;
    Ok(std::io::Cursor::new(bytes.to_vec()))
}

pub async fn partitions_from_path_iterator<'a, F, T, I>(
    path_iter: I,
    mut partition_reader: F,
    dfctx: &datafusion::execution::context::SessionContext,
) -> Result<Vec<T>, io::Error>
where
    I: Iterator<Item = &'a str>,
    F: FnMut(std::io::Cursor<Vec<u8>>) -> Result<T, table::Error>,
{
    let object_store_registry = dfctx.runtime_env().object_store_registry.clone();
    let mut partitions = vec![];

    for path_str in path_iter {
        // FIXME: fix these
        let url = &Url::from_str(path_str).unwrap();
        let client = object_store_registry
            .get_store(url)
            .context(GetStoreSnafu {
                url: url.to_owned(),
            })?;
        let path = object_store::path::Path::from(&url.path()[1..]);
        let reader = partition_key_to_reader(client.clone(), &path).await?;
        partitions.push(partition_reader(reader).context(TableSnafu)?);
    }

    Ok(partitions)
}

pub async fn partitions_from_uri<'a, F, T>(
    t: &'a TableSource,
    _uri: URIReference<'a>,
    mut partition_reader: F,
    dfctx: &datafusion::execution::context::SessionContext,
) -> Result<Vec<T>, io::Error>
where
    F: FnMut(std::io::Cursor<Vec<u8>>) -> Result<T, table::Error>,
{
    let object_store_registry = dfctx.runtime_env().object_store_registry.clone();
    let url = &Url::from_str(t.get_uri_str()).unwrap();
    let client = object_store_registry
        .get_store(url)
        .context(GetStoreSnafu {
            url: url.to_owned(),
        })?;
    let mut partitions = vec![];

    // url.path starts with "/", but object_store does not expect "/" at the beginning
    // decode percent: https://github.com/apache/arrow-datafusion/pull/3750/files
    let decoded_path = percent_encoding::percent_decode_str(&url.path()[1..]).decode_utf8_lossy();
    let path = object_store::path::Path::from(decoded_path.as_ref());

    // first try loading table uri as single object
    match partition_key_to_reader(client.clone(), &path).await {
        Ok(reader) => {
            partitions.push(partition_reader(reader).context(TableSnafu)?);
        }
        Err(e) => {
            debug!("`{path}` is not an object, try to list as a directory: {e}");
            // fallback to directory listing
            let paths = client
                .clone()
                .list(Some(&path))
                .await
                .context(ListObjectSnafu {
                    path: path.to_owned(),
                })?
                .map_ok(|meta| meta.location)
                .try_collect::<Vec<object_store::path::Path>>()
                .await
                .unwrap();
            for f in paths {
                let reader = partition_key_to_reader(client.clone(), &f).await?;
                partitions.push(partition_reader(reader).context(TableSnafu)?);
            }
        }
    }

    Ok(partitions)
}
