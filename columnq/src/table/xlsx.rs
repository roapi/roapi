use crate::{error::ColumnQError, table::TableSource};
use calamine::{open_workbook, Range, Reader, Xlsx};
use datafusion::arrow::array::{ArrayRef, BooleanArray, PrimitiveArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::vec;

fn infer_value_type(v: &calamine::DataType) -> Result<DataType, ColumnQError> {
    match v {
        calamine::DataType::Int(_) if v.get_int().is_some() => Ok(DataType::Int64),
        calamine::DataType::Float(_) if v.get_float().is_some() => Ok(DataType::Float64),
        calamine::DataType::Bool(_) if v.get_bool().is_some() => Ok(DataType::Boolean),
        calamine::DataType::String(_) if v.get_string().is_some() => Ok(DataType::Utf8),
        calamine::DataType::Error(e) => Err(ColumnQError::LoadXlsx(e.to_string())),
        // TODO(upstream): support `Date64`
        calamine::DataType::DateTime(_) => Err(ColumnQError::LoadXlsx(
            "Unsupported data type: DateTime".to_owned(),
        )),
        calamine::DataType::Empty => Ok(DataType::Null),
        _ => Err(ColumnQError::LoadXlsx(
            "Failed to parse the cell value".to_owned(),
        )),
    }
}

fn infer_schema(r: &Range<calamine::DataType>) -> Result<Schema, ColumnQError> {
    let mut col_types: HashMap<&str, HashSet<DataType>> = HashMap::new();
    let mut rows = r.rows();
    let col_names: Result<Vec<&str>, _> = rows
        .next()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(i, c)| {
            c.get_string()
                .ok_or_else(|| ColumnQError::LoadXlsx(format!("The {i}th column name is empty")))
        })
        .collect();

    let col_names = match col_names {
        Ok(values) => values,
        Err(e) => return Err(e),
    };

    for row in rows {
        for (i, col_val) in row.iter().enumerate() {
            let col_name = col_names.get(i).unwrap();
            let col_type = infer_value_type(col_val).unwrap();
            let entry = col_types.entry(col_name).or_insert_with(HashSet::new);
            entry.insert(col_type);
        }
    }

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
    Ok(Schema::new(fields))
}

fn xlsx_sheet_value_to_record_batch(
    r: Range<calamine::DataType>,
) -> Result<RecordBatch, ColumnQError> {
    let schema = infer_schema(&r)?;
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
                _ => Arc::new(
                    rows.map(|r| r.get(i).map(|v| v.get_string().unwrap_or("null")))
                        .collect::<StringArray>(),
                ) as ArrayRef,
            }
        })
        .collect::<Vec<ArrayRef>>();
    Ok(RecordBatch::try_new(Arc::new(schema), arrays)?)
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
    let mut workbook: Xlsx<_> = open_workbook(uri)
        .map_err(|_| ColumnQError::LoadXlsx("Failed to load .xlsx file".to_owned()))?;
    match &opt.sheet_name {
        Some(sheet) => {
            if let Some(Ok(r)) = workbook.worksheet_range(sheet) {
                let batch = xlsx_sheet_value_to_record_batch(r)?;
                let schema_ref = batch.schema();
                let partitions = vec![vec![batch]];
                Ok(datafusion::datasource::MemTable::try_new(
                    schema_ref, partitions,
                )?)
            } else {
                Err(ColumnQError::LoadXlsx(
                    "Failed to open .xlsx file.".to_owned(),
                ))
            }
        }
        None => Err(ColumnQError::LoadXlsx(
            "`sheet_name` is not specified".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use crate::table::TableIoSource;
    use crate::test_util::*;
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;

    use calamine::{Cell, DataType as XlsxDataType, Range};

    fn property_sheet() -> Range<XlsxDataType> {
        let cells: Vec<Cell<XlsxDataType>> = vec![
            Cell::new((0, 0), XlsxDataType::String("float_column".to_string())),
            Cell::new((1, 0), XlsxDataType::Float(1.333)),
            Cell::new((2, 0), XlsxDataType::Float(3.333)),
            Cell::new((0, 1), XlsxDataType::String("integer_column".to_string())),
            Cell::new((1, 1), XlsxDataType::Int(1)),
            Cell::new((2, 1), XlsxDataType::Int(3)),
            Cell::new((0, 2), XlsxDataType::String("boolean_column".to_string())),
            Cell::new((1, 2), XlsxDataType::Bool(true)),
            Cell::new((2, 2), XlsxDataType::Bool(false)),
            Cell::new((0, 3), XlsxDataType::String("string_column".to_string())),
            Cell::new((1, 3), XlsxDataType::String("foo".to_string())),
            Cell::new((2, 3), XlsxDataType::String("bar".to_string())),
        ];
        calamine::Range::<calamine::DataType>::from_sparse(cells)
    }

    #[tokio::test]
    async fn load_xlsx_with_toml_config() {
        let mut table_source: TableSource = toml::from_str(
            r#"
name = "test"
uri = "test_data/uk_cities_with_headers.xlsx"
[option]
format = "xlsx"
sheet_name = "uk_cities_with_headers"
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

    #[tokio::test]
    async fn load_xlsx_with_yaml_config() {
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

    #[test]
    fn schema_interface() {
        let sheet = property_sheet();
        let schema = infer_schema(&sheet).unwrap();
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("float_column", DataType::Float64, true),
                Field::new("integer_column", DataType::Int64, true),
                Field::new("boolean_column", DataType::Boolean, true),
                Field::new("string_column", DataType::Utf8, true),
            ])
        );
    }

    #[test]
    fn xlsx_value_to_record_batch() {
        let sheet = property_sheet();
        let rb = xlsx_sheet_value_to_record_batch(sheet).unwrap();

        assert_eq!(rb.num_columns(), 4);
        assert_eq!(
            rb.column(0).as_ref(),
            Arc::new(Float64Array::from(vec![1.333, 3.333])).as_ref(),
        );
        assert_eq!(
            rb.column(1).as_ref(),
            Arc::new(Int64Array::from(vec![1, 3])).as_ref(),
        );
        assert_eq!(
            rb.column(2).as_ref(),
            Arc::new(BooleanArray::from(vec![true, false])).as_ref(),
        );
        assert_eq!(
            rb.column(3).as_ref(),
            Arc::new(StringArray::from(vec!["foo", "bar"])).as_ref(),
        );
    }
}
