use std::collections::HashMap;
use std::sync::Arc;

use datafusion::datasource::TableProvider;

use crate::error::{ColumnQError, QueryError};
use crate::query;
use crate::table::{self, TableSource};

pub struct ColumnQ {
    dfctx: datafusion::execution::context::ExecutionContext,
    schema_map: HashMap<String, arrow::datatypes::SchemaRef>,
}

impl ColumnQ {
    pub fn new() -> Self {
        let dfctx = datafusion::execution::context::ExecutionContext::new();
        let schema_map = HashMap::<String, arrow::datatypes::SchemaRef>::new();
        Self { dfctx, schema_map }
    }

    pub async fn load_table(&mut self, t: &TableSource) -> Result<(), ColumnQError> {
        let table = table::load(&t).await?;
        self.schema_map.insert(t.name.clone(), table.schema());
        self.dfctx
            .register_table(t.name.as_str(), Arc::new(table))?;

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
}

impl Default for ColumnQ {
    fn default() -> Self {
        Self::new()
    }
}
