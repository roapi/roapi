use crate::{error::ColumnQError, table::TableSource};
use calamine::{open_workbook, Range, Reader, Xlsx};
use datafusion::arrow::array::{ArrayRef, BooleanArray, PrimitiveArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::vec;

fn infer_value_type(v: &calamine::DataType) -> Result<DataType, ()> {
    match v {
        calamine::DataType::Int(_) if v.get_int().is_some() => Ok(DataType::Int64),
        calamine::DataType::Float(_) if v.get_float().is_some() => Ok(DataType::Float64),
        calamine::DataType::Bool(_) if v.get_bool().is_some() => Ok(DataType::Boolean),
        calamine::DataType::Empty => Ok(DataType::Null),
        calamine::DataType::String(_) if v.get_string().is_some() => Ok(DataType::Utf8),
        calamine::DataType::DateTime(..) => todo!(),
        calamine::DataType::Error(..) => todo!(),
        _ => todo!(),
    }
}

fn infer_schema(r: Range<calamine::DataType>) -> Schema {
    let mut col_types: HashMap<&str, HashSet<DataType>> = HashMap::new();
    let mut rows = r.rows();
    let col_names: Vec<&str> = rows
        .nth(0)
        .unwrap()
        .iter()
        .map(|c| c.get_string().unwrap())
        .collect();
    rows.skip(1).for_each(|row| {
        row.iter().enumerate().for_each(|(i, col_val)| {
            let col_name = col_names[i];
            let col_type = infer_value_type(col_val).unwrap();
            let entry = col_types.entry(col_name).or_insert_with(HashSet::new);
            entry.insert(col_type);
        });
    });

    let fields: Vec<Field> = col_names
        .iter()
        .map(|col_name| {
            let set = col_types.entry(col_name).or_insert_with(|| {
                let mut set = HashSet::new();
                set.insert(DataType::Utf8);
                set
            });

            let mut dt_iter = set.iter().cloned();
            let dt = dt_iter.next().unwrap_or(DataType::Utf8);
            Field::new(&col_name.replace(' ', "_"), dt, true)
        })
        .collect();
    Schema::new(fields)
}

fn xlsx_sheet_value_to_record_batch(r: Range<calamine::DataType>) -> RecordBatch {
    let schema = infer_schema(r.clone());
    let arrays = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let rows = r.rows().skip(1);
            match field.data_type() {
                DataType::Boolean => Arc::new(
                    rows.map(|r| r.get(i).map(|v| v.get_bool().unwrap()))
                        .collect::<BooleanArray>(),
                ) as ArrayRef,
                DataType::Int64 => Arc::new(
                    rows.map(|r| r.get(i).map(|v| v.get_int().unwrap()))
                        .collect::<PrimitiveArray<Int64Type>>(),
                ) as ArrayRef,
                DataType::Float64 => Arc::new(
                    rows.map(|r| r.get(i).map(|v| v.get_float().unwrap()))
                        .collect::<PrimitiveArray<Float64Type>>(),
                ) as ArrayRef,
                DataType::Utf8 => Arc::new(
                    rows.map(|r| r.get(i).map(|v| v.get_string().unwrap()))
                        .collect::<StringArray>(),
                ) as ArrayRef,
                _ => todo!(),
            }
        })
        .collect::<Vec<ArrayRef>>();
    RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
}

pub async fn to_mem_table(
    t: &TableSource,
) -> Result<datafusion::datasource::MemTable, ColumnQError> {
    let opt = t
        .option
        .as_ref()
        .ok_or(ColumnQError::MissingOption)?
        .as_xlsx()?;
    let uri = t.get_uri_str();
    let mut workbook: Xlsx<_> = open_workbook(uri).unwrap();
    match &opt.sheet_name {
        Some(sheet) => {
            if let Some(Ok(r)) = workbook.worksheet_range(sheet) {
                let batch = xlsx_sheet_value_to_record_batch(r);
                let schema_ref = batch.schema();
                let partitions = vec![vec![batch]];
                return Ok(
                    datafusion::datasource::MemTable::try_new(schema_ref, partitions).unwrap(),
                );
            }
        }
        None => todo!(),
    }
    todo!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::TableIoSource;
    use crate::test_util::*;
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn load_xlsx_with_config() {
        let mut table_source: TableSource = serde_yaml::from_str(
            r#"
name: "test"
uri: "test_data/uk_cities_with_headers.xlsx"
option:
  format: "xlsx"
  sheet_name: "uk_cities_with_headers"
"#,
        )
        .unwrap();
        // patch uri path with the correct test data path
        table_source.io_source = TableIoSource::Uri(test_data_path("uk_cities_with_headers.xlsx"));
        let t = to_mem_table(&table_source).await.unwrap();
        let ctx = SessionContext::new();
        let stats = t
            .scan(&ctx.state(), &None, &[], None)
            .await
            .unwrap()
            .statistics();
        assert_eq!(stats.num_rows, Some(37));
    }
}
