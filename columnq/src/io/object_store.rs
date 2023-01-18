use futures::TryStreamExt;
use std::str::FromStr;
use url::Url;
use crate::table::TableSource;
use uriparse::URIReference;
use datafusion::datasource::object_store::ObjectStoreProvider;
use std::sync::Arc;
use crate::error::ColumnQError;
use object_store::ObjectStore;
use crate::columnq::ColumnQObjectStoreProvider;

pub async fn partition_key_to_reader(
    client: Arc<dyn ObjectStore>,
    path: &object_store::path::Path,
) -> Result<std::io::Cursor<Vec<u8>>, ColumnQError> {
    let get_result = client
        .get(path)
        .await?;
    let bytes = get_result
        .bytes()
        .await?;
    Ok(std::io::Cursor::new(bytes.to_vec()))
}

pub async fn partitions_from_path_iterator<'a, F, T, I>(
    path_iter: I,
    mut partition_reader: F,
) -> Result<Vec<T>, ColumnQError>
where
    I: Iterator<Item = &'a str>,
    F: FnMut(std::io::Cursor<Vec<u8>>) -> Result<T, ColumnQError>,
{
    let object_store_provider = ColumnQObjectStoreProvider {};
    let mut partitions = vec![];

    for path_str in path_iter {
        let url = &Url::from_str(path_str).unwrap();
        let client = object_store_provider.get_by_url(url)?;
        let path = object_store::path::Path::from(&url.path()[1..]);
        let reader = partition_key_to_reader(client.clone(), &path).await?;
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
    let object_store_provider = ColumnQObjectStoreProvider {};
    let url = &Url::from_str(t.get_uri_str()).unwrap();
    let client = object_store_provider.get_by_url(url)?;
    let mut partitions = vec![];

    // first try loading table uri as single object
    // url.path starts with "/", but object_store does not expect "/" at the beginning
    let path = object_store::path::Path::from(&url.path()[1..]);
    match partition_key_to_reader(client.clone(), &path).await {
        Ok(reader) => {
            partitions.push(partition_reader(reader)?);
        }
        Err(_) => {
            // fallback to directory listing
            let paths = client.clone()
                .list(Some(&path))
                .await?
                .map_ok(|meta| meta.location)
                .try_collect::<Vec<object_store::path::Path>>()
                .await
                .unwrap();
            for f in paths {
                let reader = partition_key_to_reader(client.clone(), &f).await?;
                partitions.push(partition_reader(reader)?);
            }
        }
    }

    Ok(partitions)
}
