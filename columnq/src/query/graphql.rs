use std::convert::TryFrom;
use std::sync::Arc;

use datafusion::arrow;
use datafusion::logical_expr::Operator;
use datafusion::prelude::{binary_expr, Column, Expr};
use datafusion::scalar::ScalarValue;
use graphql_parser::query::{parse_query, Definition, OperationDefinition, Selection, Value};

use crate::error::QueryError;
use crate::query::{column_sort_expr_asc, column_sort_expr_desc};

// GraphQL reference used: https://spec.graphql.org/June2018/

// TODO: propagate line and column numbers in error

impl From<graphql_parser::query::ParseError> for QueryError {
    fn from(error: graphql_parser::query::ParseError) -> Self {
        QueryError {
            error: "invalid graphql query".to_string(),
            message: error.to_string(),
        }
    }
}

fn invalid_selection_set(error: datafusion::error::DataFusionError) -> QueryError {
    QueryError {
        error: "invalid_selection_set".to_string(),
        message: format!("failed to apply selection set for query: {error}"),
    }
}

fn invalid_query(message: String) -> QueryError {
    QueryError {
        error: "invalid graphql query".to_string(),
        message,
    }
}

// convert order list from graphql argument to datafusion sort columns
//
// sort order matters, thus it's modeled as a list
fn to_datafusion_sort_columns<'b>(
    sort_columns: &[Value<'b, &'b str>],
) -> Result<Vec<Expr>, QueryError> {
    sort_columns
        .iter()
        .map(|optval| match optval {
            Value::Object(opt) => {
                let col = match opt.get("field") {
                    Some(Value::String(s)) => s,
                    None => {
                        return Err(invalid_query(
                            "sort option requires `field` argument".to_string(),
                        ));
                    }
                    _ => {
                        return Err(invalid_query(format!(
                            "field in sort option should be a string, got: {optval}",
                        )));
                    }
                };

                match opt.get("order") {
                    None => Ok(column_sort_expr_asc(col.to_string())),
                    Some(Value::String(s)) => match s.as_str() {
                        "desc" => Ok(column_sort_expr_desc(col.to_string())),
                        "asc" => Ok(column_sort_expr_asc(col.to_string())),
                        other => Err(invalid_query(format!(
                            "sort order needs to be either `desc` or `asc`, got: {other}",
                        ))),
                    },
                    Some(v) => Err(invalid_query(format!(
                        "sort order value should to be a String, got: {v}",
                    ))),
                }
            }
            other => Err(invalid_query(format!(
                "sort condition should be defined as object, got: {other}",
            ))),
        })
        .collect()
}

fn operand_to_datafusion_expr<'b>(operand: &Value<'b, &'b str>) -> Result<Expr, QueryError> {
    match operand {
        Value::Boolean(b) => Ok(Expr::Literal(ScalarValue::Boolean(Some(*b)))),
        Value::String(s) => Ok(Expr::Literal(ScalarValue::Utf8(Some(s.to_string())))),
        // GraphQL only supports int32 scalar input: http://spec.graphql.org/June2018/#sec-Int, but
        // graphql crate only supports in64.
        // TODO: set literal value type based on schema?
        Value::Int(n) => Ok(Expr::Literal(ScalarValue::Int64(Some(
            n.as_i64().ok_or_else(|| {
                invalid_query(format!(
                    "invalid integer number in filter predicate: {operand}"
                ))
            })?,
        )))),
        Value::Float(f) => Ok(Expr::Literal(ScalarValue::Float64(Some(f.to_owned())))),
        other => Err(invalid_query(format!(
            "invalid operand in filter predicate: {other}",
        ))),
    }
}

// graphql filter in the format of:
//
// ```
// table(
//     filter: {
//         col1: { eq: "val1" }
//         col2: { lt: 5, gt: 0 }
//         col3: "foo"
//     }
// ) {
//     col3
//     col4
// }
// ```
fn to_datafusion_predicates<'b>(
    col: &str,
    filter: &Value<'b, &'b str>,
) -> Result<Vec<Expr>, QueryError> {
    match filter {
        Value::Object(obj) => obj
            .iter()
            .map(|(op, operand)| {
                let col_expr = Expr::Column(Column::from_name(col.to_string()));
                let right_expr = operand_to_datafusion_expr(operand)?;
                match *op {
                    "eq" => Ok(binary_expr(col_expr, Operator::Eq, right_expr)),
                    "lt" => Ok(binary_expr(col_expr, Operator::Lt, right_expr)),
                    "lte" | "lteq" => Ok(binary_expr(col_expr, Operator::LtEq, right_expr)),
                    "gt" => Ok(binary_expr(col_expr, Operator::Gt, right_expr)),
                    "gte" | "gteq" => Ok(binary_expr(col_expr, Operator::GtEq, right_expr)),
                    other => Err(invalid_query(format!(
                        "invalid filter predicate operator, got: {other}",
                    ))),
                }
            })
            .collect::<Result<Vec<Expr>, _>>(),
        // when filter is literal, default to equality comparison
        Value::Boolean(_) | Value::Int(_) | Value::Float(_) | Value::String(_) => {
            Ok(vec![binary_expr(
                Expr::Column(Column::from_name(col.to_string())),
                Operator::Eq,
                operand_to_datafusion_expr(filter)?,
            )])
        }
        other => Err(invalid_query(format!(
            "filter predicate should be defined as object, got: {other}",
        ))),
    }
}

pub fn query_to_df(
    dfctx: &datafusion::execution::context::SessionContext,
    q: &str,
) -> Result<Arc<datafusion::dataframe::DataFrame>, QueryError> {
    let doc = parse_query::<&str>(q)?;

    let def = match doc.definitions.len() {
        1 => match &doc.definitions[0] {
            Definition::Operation(op_def) => op_def,
            Definition::Fragment(_) => {
                return Err(QueryError {
                    error: "invalid graphql query".to_string(),
                    message: "TODO: fragment definition not supported, please file a Github issue"
                        .to_string(),
                });
            }
        },
        0 => {
            return Err(QueryError {
                error: "invalid graphql query".to_string(),
                message: "empty query".to_string(),
            });
        }
        n => {
            return Err(QueryError {
                error: "invalid graphql query".to_string(),
                message: format!("only 1 definition allowed, got: {n}"),
            });
        }
    };

    let selections = &match def {
        OperationDefinition::Query(query) => &query.selection_set,
        OperationDefinition::SelectionSet(sel) => sel,
        _ => {
            return Err(QueryError {
                error: "invalid graphql query".to_string(),
                message: format!("Unsupported operation: {def}"),
            });
        }
    }
    .items;

    let mut field = None;
    // TODO: reenable clippy::never_loop rule after we added support for FragmentSpread and
    // InlineFragment
    #[allow(clippy::never_loop)]
    for selection in selections {
        match selection {
            Selection::Field(f) => {
                field = Some(f);
                break;
            }
            Selection::FragmentSpread(_) => {
                return Err(QueryError {
                    error: "invalid graphql query".to_string(),
                    message:
                        "TODO: Selection::FragmentSpread not supported, please file github issue"
                            .to_string(),
                });
            }
            Selection::InlineFragment(_) => {
                return Err(QueryError {
                    error: "invalid graphql query".to_string(),
                    message:
                        "TODO: Selection::InlineFragment not supported, please file github issue"
                            .to_string(),
                });
            }
        }
    }

    let field = field.ok_or_else(|| invalid_query("field not found in selection".to_string()))?;

    let mut df = dfctx
        .table(field.name)
        .map_err(|e| QueryError::invalid_table(e, field.name))?;

    let mut filter = None;
    let mut sort = None;
    let mut limit = None;
    let mut page = None;
    for (key, value) in &field.arguments {
        match *key {
            "filter" => {
                filter = Some(value);
            }
            "sort" => {
                sort = Some(value);
            }
            "limit" => {
                limit = Some(value);
            }
            "page" => page = Some(value),
            other => {
                return Err(invalid_query(format!("invalid query argument: {other}")));
            }
        }
    }

    // apply filter
    if let Some(value) = filter {
        match value {
            Value::Object(filters) => {
                for (col, filter) in filters {
                    for p in to_datafusion_predicates(col, filter)? {
                        df = df.filter(p).map_err(QueryError::invalid_filter)?;
                    }
                }
            }
            other => {
                return Err(invalid_query(format!(
                    "filter argument takes object as value, got: {other}"
                )));
            }
        }
    }

    // apply projection
    let column_names = field
        .selection_set
        .items
        .iter()
        .map(|selection| match selection {
            Selection::Field(f) => Ok(f.name),
            _ => Err(QueryError {
                error: "invalid graphql query".to_string(),
                message: "selection set in query should only contain Fields".to_string(),
            }),
        })
        .collect::<Result<Vec<&str>, _>>()?;
    df = df
        .select_columns(&column_names)
        .map_err(invalid_selection_set)?;

    // apply sort
    if let Some(value) = sort {
        match value {
            Value::List(sort_options) => {
                df = df
                    .sort(to_datafusion_sort_columns(sort_options)?)
                    .map_err(QueryError::invalid_sort)?;
            }
            other => {
                return Err(invalid_query(format!(
                    "sort argument takes list as value, got: {other}"
                )));
            }
        }
    }

    // apply limit
    // apply limit
    if let Some(value) = limit {
        match value {
            Value::Int(n) => {
                let skip = match page {
                    None => 0,
                    Some(value) => {
                        if let Value::Int(n) = value {
                            n.as_i64().ok_or_else(|| {
                                invalid_query(format!(
                                    "invalid 64bits integer number in limit argument: {value}",
                                ))
                            })? - 1
                        } else {
                            0
                        }
                    }
                };
                let limit = n.as_i64().ok_or_else(|| {
                    invalid_query(format!(
                        "invalid 64bits integer number in limit argument: {value}",
                    ))
                })?;
                df = df
                    .limit(
                        (skip as usize) * limit as usize,
                        Some(usize::try_from(limit).map_err(|_| {
                            invalid_query(format!("limit value too large: {value}"))
                        })?),
                    )
                    .map_err(QueryError::invalid_limit)?;
            }
            other => {
                return Err(invalid_query(format!(
                    "limit argument takes int as value, got: {other}",
                )));
            }
        }
    }

    Ok(df)
}

pub async fn exec_query(
    dfctx: &datafusion::execution::context::SessionContext,
    q: &str,
) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError> {
    query_to_df(dfctx, q)?
        .collect()
        .await
        .map_err(QueryError::query_exec)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::*;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::{col, lit};

    use super::*;
    use crate::test_util::*;

    #[test]
    fn simple_query_planning() -> anyhow::Result<()> {
        let mut dfctx = SessionContext::new();
        register_table_properties(&mut dfctx)?;

        let df = query_to_df(
            &dfctx,
            r#"{
                properties(
                    filter: {
                        bed: { gt: 3 }
                        bath: { gteq: 2 }
                    }
                ) {
                    address
                    bed
                    bath
                }
            }"#,
        )?;

        let expected_df = dfctx
            .table("properties")?
            .filter(col("bath").gt_eq(lit(2i64)))?
            .filter(col("bed").gt(lit(3i64)))?
            .select(vec![col("address"), col("bed"), col("bath")])?;

        assert_eq_df(df, expected_df);

        Ok(())
    }

    #[test]
    fn consistent_and_deterministics_logical_plan() -> anyhow::Result<()> {
        let mut dfctx = SessionContext::new();
        register_table_properties(&mut dfctx)?;

        let df = query_to_df(
            &dfctx,
            r#"{
                properties(
                    filter: {
                        bed: { gt: 3 }
                    }
                    limit: 10
                    sort: [
                        { field: "bed" }
                    ]
                ) {
                    address
                    bed
                }
            }"#,
        )?;

        let expected_df = dfctx
            .table("properties")?
            .filter(col("bed").gt(lit(3i64)))?
            .select(vec![col("address"), col("bed")])?
            .sort(vec![column_sort_expr_asc("bed")])?
            .limit(0, Some(10))?;

        assert_eq_df(df, expected_df);

        Ok(())
    }

    #[tokio::test]
    async fn boolean_literal_as_predicate_operand() -> anyhow::Result<()> {
        let mut dfctx = SessionContext::new();
        register_table_properties(&mut dfctx)?;

        let batches = exec_query(
            &dfctx,
            r#"{
                properties(
                    filter: {
                        occupied: false
                        bed: { gteq: 4 }
                    }
                ) {
                    address
                    bed
                    bath
                }
            }"#,
        )
        .await?;

        let batch = &batches[0];

        assert_eq!(
            batch.column(0).as_ref(),
            Arc::new(StringArray::from(vec!["Kenmore, WA", "Fremont, WA",])).as_ref(),
        );

        assert_eq!(
            batch.column(1).as_ref(),
            Arc::new(Int64Array::from(vec![4, 5])).as_ref(),
        );

        assert_eq!(
            batch.column(2).as_ref(),
            Arc::new(Int64Array::from(vec![3, 3])).as_ref(),
        );

        Ok(())
    }
}
