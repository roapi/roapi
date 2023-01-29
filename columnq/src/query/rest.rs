use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::Operator;
use datafusion::prelude::{binary_expr, Column, Expr};
use datafusion::scalar::ScalarValue;
use regex::Regex;

use crate::error::QueryError;
use crate::query::{column_sort_expr_asc, column_sort_expr_desc};

fn err_rest_query_value(error: sqlparser::tokenizer::TokenizerError) -> QueryError {
    QueryError {
        error: "rest_query_value".to_string(),
        message: format!("invalid REST query value {error:?}"),
    }
}

fn rest_query_value_to_expr(v: &str) -> Result<Expr, QueryError> {
    let dialect = sqlparser::dialect::GenericDialect {};
    let mut tokenizer = sqlparser::tokenizer::Tokenizer::new(&dialect, v);
    let tokens = tokenizer.tokenize().map_err(err_rest_query_value)?;

    let t = &tokens[0];
    match t {
        // TODO: support column expr instead of just literal
        sqlparser::tokenizer::Token::SingleQuotedString(s) => {
            Ok(Expr::Literal(ScalarValue::Utf8(Some(s.to_string()))))
        }
        sqlparser::tokenizer::Token::Number(s, _) => {
            if let Ok(n) = s.parse() {
                Ok(Expr::Literal(ScalarValue::Int64(Some(n))))
            } else if let Ok(n) = s.parse() {
                Ok(Expr::Literal(ScalarValue::Float64(Some(n))))
            } else {
                Err(QueryError {
                    error: "rest_query_value".to_string(),
                    message: format!("invalid REST query numeric value {s}"),
                })
            }
        }
        _ => Err(QueryError {
            error: "rest_query_value".to_string(),
            message: format!("invalid REST query value {v}"),
        }),
    }
}

fn num_parse_err(e: std::num::ParseIntError) -> QueryError {
    QueryError {
        error: "invalid_numeric_param".to_string(),
        message: format!("Failed to parse numeric parameter value: {e}"),
    }
}

pub fn table_query_to_df(
    dfctx: &datafusion::execution::context::SessionContext,
    table_name: &str,
    params: &HashMap<String, String>,
) -> Result<Arc<datafusion::dataframe::DataFrame>, QueryError> {
    lazy_static! {
        static ref RE_REST_FILTER: Regex =
            Regex::new(r"filter\[(?P<column>.+)\](?P<op>.+)?").unwrap();
    }

    let mut df = dfctx
        .table(table_name)
        .map_err(|e| QueryError::invalid_table(e, table_name))?;

    // filter[col1]eq='foo'
    // filter[col2]lt=2
    for (key, val) in params.iter().filter(|(k, _)| k.starts_with("filter[")) {
        match RE_REST_FILTER.captures(key) {
            Some(caps) => {
                let col_expr = match caps.name("column") {
                    Some(column) => Expr::Column(Column::from_name(column.as_str().to_string())),
                    None => {
                        return Err(QueryError {
                            error: "rest_query".to_string(),
                            message: format!("missing column from filter `{key}`"),
                        });
                    }
                };

                let predicate = match caps.name("op") {
                    None => binary_expr(col_expr, Operator::Eq, rest_query_value_to_expr(val)?),
                    Some(m) => match m.as_str() {
                        "eq" | "" => {
                            binary_expr(col_expr, Operator::Eq, rest_query_value_to_expr(val)?)
                        }
                        "lt" => binary_expr(col_expr, Operator::Lt, rest_query_value_to_expr(val)?),
                        "lte" | "lteq" => {
                            binary_expr(col_expr, Operator::LtEq, rest_query_value_to_expr(val)?)
                        }
                        "gt" => binary_expr(col_expr, Operator::Gt, rest_query_value_to_expr(val)?),
                        "gte" | "gteq" => {
                            binary_expr(col_expr, Operator::GtEq, rest_query_value_to_expr(val)?)
                        }
                        _ => {
                            return Err(QueryError {
                                error: "rest_query".to_string(),
                                message: format!("unsupported filter operator {}", m.as_str()),
                            });
                        }
                    },
                };

                df = df.filter(predicate).map_err(QueryError::invalid_filter)?;
            }
            None => {
                return Err(QueryError {
                    error: "rest_query".to_string(),
                    message: format!("invalid filter condition {key}"),
                });
            }
        }
    }

    // columns=col1,col2,col3
    if let Some(val) = params.get("columns") {
        let column_names = val.split(',').collect::<Vec<_>>();
        df = df
            .select_columns(&column_names)
            .map_err(QueryError::invalid_projection)?;
    }

    // sort=col1,-col2
    // - denotes DESC sort order
    if let Some(val) = params.get("sort") {
        let sort_columns = val.split(',');
        let sort_exprs = sort_columns
            .map(|val| match val.chars().next() {
                Some('-') => column_sort_expr_desc(val[1..].to_string()),
                Some('+') => column_sort_expr_asc(val[1..].to_string()),
                _ => column_sort_expr_asc(val.to_string()),
            })
            .collect::<Vec<_>>();
        df = df.sort(sort_exprs).map_err(QueryError::invalid_sort)?;
    }

    // limit=100
    // limit needs to be applied after sort to make sure the result is deterministics
    if let Some(val) = params.get("limit") {
        let limit = val.parse::<usize>().map_err(num_parse_err)?;
        if let Some(val) = params.get("page") {
            let skip = (val.parse::<usize>().map_err(num_parse_err)? - 1) * limit;
            df = df
                .limit(skip, Some(limit))
                .map_err(QueryError::invalid_limit)?;
        } else {
            df = df
                .limit(0, Some(limit))
                .map_err(QueryError::invalid_limit)?;
        }
    }

    Ok(df)
}

pub async fn query_table(
    dfctx: &datafusion::execution::context::SessionContext,
    table_name: &str,
    params: &HashMap<String, String>,
) -> Result<Vec<RecordBatch>, QueryError> {
    let df = table_query_to_df(dfctx, table_name, params)?;
    df.collect().await.map_err(QueryError::query_exec)
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::*;
    use datafusion::execution::context::SessionContext;
    use datafusion::prelude::*;

    use crate::test_util::*;

    #[tokio::test]
    async fn consistent_and_deterministics_logical_plan() -> anyhow::Result<()> {
        let mut dfctx = SessionContext::new();
        register_table_ubuntu_ami(&mut dfctx).await?;

        let mut params = HashMap::<String, String>::new();
        params.insert("limit".to_string(), "10".to_string());
        params.insert("sort".to_string(), "ami_id".to_string());
        params.insert("columns".to_string(), "ami_id,version".to_string());
        params.insert("filter[arch]".to_string(), "'amd64'".to_string());

        let df = table_query_to_df(&dfctx, "ubuntu_ami", &params)?;

        assert_eq_df(
            df,
            dfctx
                .table("ubuntu_ami")?
                .filter(
                    col("arch").eq(Expr::Literal(ScalarValue::Utf8(Some("amd64".to_string())))),
                )?
                .select(vec![col("ami_id"), col("version")])?
                .sort(vec![column_sort_expr_asc("ami_id")])?
                .limit(0, Some(10))?,
        );

        Ok(())
    }

    #[tokio::test]
    async fn simple_filter() -> anyhow::Result<()> {
        let mut dfctx = SessionContext::new();
        register_table_ubuntu_ami(&mut dfctx).await?;
        let mut params = HashMap::<String, String>::new();

        params.insert("columns".to_string(), "ami_id".to_string());
        params.insert("filter[version]".to_string(), "'20.10'".to_string());
        params.insert("filter[arch]".to_string(), "'amd64'".to_string());
        params.insert("filter[zone]".to_string(), "'us-east-2'".to_string());

        let batches = query_table(&dfctx, "ubuntu_ami", &params).await?;

        let batch = &batches[0];
        assert_eq!(
            batch.column(0).as_ref(),
            Arc::new(StringArray::from(vec!["<a href=\"https://console.aws.amazon.com/ec2/home?region=us-east-2#launchAmi=ami-091a87cd1ff23d97c\">ami-091a87cd1ff23d97c</a>"])).as_ref(),
        );

        Ok(())
    }
}
