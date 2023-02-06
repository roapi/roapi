use crate::io::BlobStoreType;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::array::as_string_array;
use datafusion::arrow::array::StringArray;
use datafusion::datasource::object_store::{ObjectStoreProvider, ObjectStoreRegistry};
use datafusion::error::{DataFusionError, Result as DatafusionResult};
pub use datafusion::execution::context::SessionConfig;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::collect;

use crate::error::{ColumnQError, QueryError};
use crate::query;
use crate::table::{self, KeyValueSource, TableSource};
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use url::Url;

pub struct ColumnQObjectStoreProvider {}
impl ObjectStoreProvider for ColumnQObjectStoreProvider {
    fn get_by_url(&self, url: &Url) -> DatafusionResult<Arc<dyn object_store::ObjectStore>> {
        match url.host_str() {
            None => Err(DataFusionError::Execution(format!(
                "Missing bucket name: {}",
                url.as_str()
            ))),
            Some(host) => {
                let url_schema = url.scheme();
                match BlobStoreType::try_from(url_schema) {
                    Err(err) => Err(DataFusionError::External(Box::new(err))),
                    Ok(blob_type) => match blob_type {
                        BlobStoreType::S3 => {
                            let mut s3_builder = AmazonS3Builder::from_env().with_bucket_name(host);
                            // for minio in CI
                            s3_builder = s3_builder.with_allow_http(true);

                            match s3_builder.build() {
                                Ok(s3) => Ok(Arc::new(s3)),
                                Err(err) => Err(DataFusionError::External(Box::new(err))),
                            }
                        }
                        BlobStoreType::GCS => {
                            let gcs_builder =
                                GoogleCloudStorageBuilder::from_env().with_bucket_name(host);
                            match gcs_builder.build() {
                                Ok(gcs) => Ok(Arc::new(gcs)),
                                Err(err) => Err(DataFusionError::External(Box::new(err))),
                            }
                        }
                        BlobStoreType::Azure => {
                            let azure_builder =
                                MicrosoftAzureBuilder::from_env().with_container_name(host);
                            match azure_builder.build() {
                                Ok(azure) => Ok(Arc::new(azure)),
                                Err(err) => Err(DataFusionError::External(Box::new(err))),
                            }
                        }
                        _ => Err(DataFusionError::Execution(format!(
                            "Unsupported scheme: {url_schema}"
                        ))),
                    },
                }
            }
        }
    }
}

pub struct ColumnQ {
    pub dfctx: SessionContext,
    schema_map: HashMap<String, arrow::datatypes::SchemaRef>,
    kv_catalog: HashMap<String, Arc<HashMap<String, String>>>,
}

impl ColumnQ {
    pub fn new() -> Self {
        Self::new_with_config(SessionConfig::from_env().with_information_schema(true))
    }

    pub fn new_with_config(config: SessionConfig) -> Self {
        let object_store_provider = ColumnQObjectStoreProvider {};
        let object_store_registry =
            ObjectStoreRegistry::new_with_provider(Some(Arc::new(object_store_provider)));
        let rn_config =
            RuntimeConfig::new().with_object_store_registry(Arc::new(object_store_registry));
        let runtime_env = RuntimeEnv::new(rn_config).unwrap();
        let dfctx = SessionContext::with_config_rt(config, Arc::new(runtime_env));

        let schema_map = HashMap::<String, arrow::datatypes::SchemaRef>::new();
        Self {
            dfctx,
            schema_map,
            kv_catalog: HashMap::new(),
        }
    }

    pub async fn load_table(&mut self, t: &TableSource) -> Result<(), ColumnQError> {
        let table = table::load(t, &self.dfctx).await?;
        self.schema_map.insert(t.name.clone(), table.schema());
        self.dfctx.deregister_table(t.name.as_str())?;
        self.dfctx.register_table(t.name.as_str(), table)?;

        Ok(())
    }

    pub async fn load_kv(&mut self, kv: KeyValueSource) -> Result<(), ColumnQError> {
        use datafusion::arrow::datatypes::DataType;

        let kv_entry = self.kv_catalog.entry(kv.name.clone());
        let (key, value) = (kv.key.clone(), kv.value.clone());
        let table = table::load(&kv.into(), &self.dfctx).await?;
        let schema = table.schema();
        let key_schema_idx = schema.index_of(&key)?;
        if schema.field(key_schema_idx).data_type() != &DataType::Utf8 {
            return Err(ColumnQError::invalid_kv_key_type());
        }
        let val_schema_idx = schema.index_of(&value)?;
        let projections = Some(vec![key_schema_idx, val_schema_idx]);

        let filters = &[];
        let exec_plan = table
            .scan(&self.dfctx.state(), &projections, filters, None)
            .await?;
        let batches = collect(exec_plan, self.dfctx.task_ctx()).await?;
        let mut kv = HashMap::new();
        for batch in batches {
            let col_key = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(ColumnQError::invalid_kv_key_type)?;
            // TODO: take ownership of array data
            let key_iter = col_key.iter();
            let col_val = batch.column(1);
            match col_val.data_type() {
                DataType::Utf8 => {
                    let val_iter = as_string_array(col_val).iter();
                    key_iter
                        .zip(val_iter)
                        .for_each(|(key, val): (Option<&str>, Option<&str>)| {
                            // TODO: support null as value? error out on null?
                            if let (Some(key), Some(val)) = (key, val) {
                                kv.insert(key.to_string(), val.to_string());
                            }
                        });
                }
                other => {
                    todo!("unsupported type: {}", other);
                }
            }
        }

        match kv_entry {
            Entry::Occupied(mut entry) => {
                entry.insert(Arc::new(kv));
            }
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(kv));
            }
        }
        Ok(())
    }

    pub fn schema_map(&self) -> &HashMap<String, arrow::datatypes::SchemaRef> {
        &self.schema_map
    }

    pub async fn query_graphql(
        &self,
        query: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError> {
        query::graphql::exec_query(&self.dfctx, query).await
    }

    pub async fn query_sql(
        &self,
        query: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError> {
        query::sql::exec_query(&self.dfctx, query).await
    }

    pub async fn query_rest_table(
        &self,
        table_name: &str,
        params: &HashMap<String, String>,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError> {
        query::rest::query_table(&self.dfctx, table_name, params).await
    }

    pub fn kv_get(&self, kv_name: &str, key: &str) -> Result<Option<&String>, QueryError> {
        let map = self
            .kv_catalog
            .get(kv_name)
            .ok_or_else(|| QueryError::invalid_kv_name(kv_name))?;
        Ok(map.get(key))
    }
}

impl Default for ColumnQ {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::{env, str::FromStr};
    use tempfile::Builder;

    use datafusion::datasource::object_store::ObjectStoreProvider;
    use url::Url;

    use super::ColumnQObjectStoreProvider;

    #[test]
    fn s3_object_store_type() {
        let host_url = "s3://bucket_name/path";
        let provider = ColumnQObjectStoreProvider {};

        let err = provider
            .get_by_url(&Url::from_str(host_url).unwrap())
            .unwrap_err();
        assert!(err.to_string().contains("Generic S3 error: Missing region"));

        env::set_var("AWS_REGION", "us-east-1");
        let res = provider.get_by_url(&Url::from_str(host_url).unwrap());
        let msg = match res {
            Err(e) => format!("{e}"),
            Ok(_) => "".to_string(),
        };
        assert_eq!("".to_string(), msg);
        env::remove_var("AWS_REGION");
    }

    #[test]
    fn s3_object_store_type_no_bucket() {
        let host_url = "s3://";
        let provider = ColumnQObjectStoreProvider {};

        let err = provider
            .get_by_url(&Url::from_str(host_url).unwrap())
            .unwrap_err();
        assert!(err.to_string().contains("Missing bucket name: s3://"));
    }

    #[tokio::test]
    async fn gcs_object_store_type() -> anyhow::Result<()> {
        let host_url = "gs://bucket_name/path";
        let provider = ColumnQObjectStoreProvider {};

        let tmp_dir = Builder::new().prefix("columnq.test.gcs").tempdir()?;
        let tmp_gcs_path = tmp_dir.path().join("service_account.json");
        let mut tmp_gcs = File::create(tmp_gcs_path.clone())?;
        writeln!(
            tmp_gcs,
            r#"{{"gcs_base_url": "http://localhost:4443", "disable_oauth": true, "client_email": "", "private_key": ""}}"#
        )?;
        env::set_var("GOOGLE_SERVICE_ACCOUNT", tmp_gcs_path);

        let res = provider.get_by_url(&Url::from_str(host_url).unwrap());
        let msg = match res {
            Err(e) => format!("{e}"),
            Ok(_) => "".to_string(),
        };
        assert_eq!("".to_string(), msg);

        drop(tmp_gcs);
        tmp_dir.close()?;
        env::remove_var("GOOGLE_SERVICE_ACCOUNT");
        Ok(())
    }

    #[test]
    fn azure_object_store_type() {
        let host_url = "az://bucket_name/path";
        let provider = ColumnQObjectStoreProvider {};
        // https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio#http-connection-strings
        env::set_var("AZURE_STORAGE_ACCOUNT_NAME", "devstoreaccount1");
        env::set_var("AZURE_STORAGE_ACCOUNT_KEY", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");

        let res = provider.get_by_url(&Url::from_str(host_url).unwrap());
        let msg = match res {
            Err(e) => format!("{e}"),
            Ok(_) => "".to_string(),
        };
        assert_eq!("".to_string(), msg);

        env::remove_var("AZURE_STORAGE_ACCOUNT_NAME");
        env::remove_var("AZURE_STORAGE_ACCOUNT_KEY");
    }

    #[test]
    fn unknown_object_store_type() {
        let unknown = "unknown://bucket_name/path";
        let provider = ColumnQObjectStoreProvider {};
        let err = provider
            .get_by_url(&Url::from_str(unknown).unwrap())
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("External error: Invalid table URI: Unsupported scheme: \"unknown\""))
    }
}
