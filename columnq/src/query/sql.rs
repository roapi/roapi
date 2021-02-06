use std::sync::Arc;

use crate::error::QueryError;

pub async fn exec_query(
    dfctx: &datafusion::execution::context::ExecutionContext,
    sql: &str,
) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError> {
    let plan = dfctx
        .create_logical_plan(sql)
        .map_err(QueryError::plan_sql)?;

    let df: Arc<dyn datafusion::dataframe::DataFrame> = Arc::new(
        datafusion::execution::dataframe_impl::DataFrameImpl::new(dfctx.state.clone(), &plan),
    );

    df.collect().await.map_err(QueryError::query_exec)
}
