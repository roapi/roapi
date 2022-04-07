use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::array::as_string_array;
use datafusion::arrow::array::StringArray;
pub use datafusion::execution::context::SessionConfig;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::collect;

use crate::error::{ColumnQError, QueryError};
use crate::query;
use crate::table::{self, KeyValueSource, TableSource};

pub struct ColumnQ {
    dfctx: SessionContext,
    schema_map: HashMap<String, arrow::datatypes::SchemaRef>,
    kv_catalog: HashMap<String, Arc<HashMap<String, String>>>,
}

impl ColumnQ {
    pub fn new() -> Self {
        Self::new_with_config(SessionConfig::default())
    }

    pub fn new_with_config(config: SessionConfig) -> Self {
        let dfctx = SessionContext::with_config(config);
        let schema_map = HashMap::<String, arrow::datatypes::SchemaRef>::new();
        Self {
            dfctx,
            schema_map,
            kv_catalog: HashMap::new(),
        }
    }

    pub async fn load_table(&mut self, t: &TableSource) -> Result<(), ColumnQError> {
        let table = table::load(t).await?;
        self.schema_map.insert(t.name.clone(), table.schema());
        self.dfctx.deregister_table(t.name.as_str())?;
        self.dfctx.register_table(t.name.as_str(), table)?;

        Ok(())
    }

    pub async fn load_kv(&mut self, kv: KeyValueSource) -> Result<(), ColumnQError> {
        use datafusion::arrow::datatypes::DataType;

        let kv_entry = self.kv_catalog.entry(kv.name.clone());
        let (key, value) = (kv.key.clone(), kv.value.clone());
        let table = table::load(&kv.into()).await?;
        let schema = table.schema();
        let key_schema_idx = schema.index_of(&key)?;
        if schema.field(key_schema_idx).data_type() != &DataType::Utf8 {
            return Err(ColumnQError::invalid_kv_key_type());
        }
        let val_schema_idx = schema.index_of(&value)?;
        let projections = Some(vec![key_schema_idx, val_schema_idx]);

        let filters = &[];
        let exec_plan = table.scan(&projections, filters, None).await?;
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
