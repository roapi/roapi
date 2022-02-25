use std::collections::HashMap;

use datafusion::arrow;
pub use datafusion::execution::context::ExecutionConfig;
use datafusion::execution::context::ExecutionContext;

use crate::error::{ColumnQError, QueryError};
use crate::query;
use crate::table::{self, TableSource};

pub struct ColumnQ {
    dfctx: ExecutionContext,
    schema_map: HashMap<String, arrow::datatypes::SchemaRef>,
}

impl ColumnQ {
    pub fn new() -> Self {
        Self::new_with_config(ExecutionConfig::default())
    }

    pub fn new_with_config(config: ExecutionConfig) -> Self {
        let dfctx = ExecutionContext::with_config(config);
        let schema_map = HashMap::<String, arrow::datatypes::SchemaRef>::new();
        Self { dfctx, schema_map }
    }

    pub async fn load_table(&mut self, t: &TableSource) -> Result<(), ColumnQError> {
        let table = table::load(t).await?;
        self.schema_map.insert(t.name.clone(), table.schema());
        self.dfctx.register_table(t.name.as_str(), table)?;

        Ok(())
    }

    pub fn schema_map(&self) -> &HashMap<String, arrow::datatypes::SchemaRef> {
        &self.schema_map
    }

    pub fn serializable_schema_map(&self) -> HashMap<&String, &arrow::datatypes::Schema> {
        self.schema_map
            .iter()
            .map(|(k, v)| (k, v.as_ref()))
            .collect()
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
}

impl Default for ColumnQ {
    fn default() -> Self {
        Self::new()
    }
}
