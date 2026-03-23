use calamine::{
    open_workbook_auto, Data as ExcelData, DataType as CalamineDataType, Range, Reader, Sheets,
};
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, DurationSecondArray, NullArray, PrimitiveArray, StringArray,
    TimestampSecondArray,
};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Field, Float64Type, Int64Type, Schema, TimeUnit,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use crate::table::{self, LoadedTable, TableOptionExcel, TableSchema, TableSource};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to load Excel: {msg}"))]
    Load { msg: String },
    #[snafu(display("Incorrect schema: {msg}"))]
    IncorrectSchema { msg: String },
    #[snafu(display("Excel schema inference error"))]
    SchemaInference,
    #[snafu(display("Failed to create record batch: {source}"))]
    CreateRecordBatch {
        source: datafusion::arrow::error::ArrowError,
    },
    #[snafu(display("Failed to open workbook: {source}"))]
    OpenWorkbook { source: calamine::Error },
}

struct ExcelSubrange<'a> {
    rows: calamine::Rows<'a, ExcelData>,
    columns_range_start: usize,
    columns_range_end: usize,
    total_rows: usize,
    current_row_id: usize,
}

impl<'a> ExcelSubrange<'a> {
    fn new(
        range: &'a Range<ExcelData>,
        rows_range_start: Option<usize>,
        rows_range_end: Option<usize>,
        columns_range_start: Option<usize>,
        columns_range_end: Option<usize>,
    ) -> ExcelSubrange<'a> {
        let rows_range_start = rows_range_start.unwrap_or(usize::MIN);
        let rows_range_end = rows_range_end
            .or(range.end().map(|v| v.0 as usize))
            .unwrap();

        let mut rows = range.rows();
        if rows_range_start > 0 {
            // rows skipping
            rows.nth(rows_range_start - 1);
        }

        ExcelSubrange {
            rows,
            columns_range_start: columns_range_start.unwrap_or(usize::MIN),
            columns_range_end: columns_range_end.unwrap_or(usize::MAX),
            total_rows: rows_range_end - rows_range_start + 1,
            current_row_id: 0,
        }
    }

    fn size(&self) -> usize {
        self.total_rows
    }
}

impl<'a> Iterator for ExcelSubrange<'a> {
    type Item = &'a [ExcelData];

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_row_id < self.total_rows {
            self.current_row_id += 1;
            self.rows
                .next()
                .map(|x| &x[self.columns_range_start..=self.columns_range_end.min(x.len() - 1)])
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.total_rows))
    }
}

fn infer_value_type(v: &ExcelData) -> Result<DataType, Error> {
    match v {
        ExcelData::Int(_) => Ok(DataType::Int64),
        ExcelData::Float(_) => Ok(DataType::Float64),
        ExcelData::String(_) => Ok(DataType::Utf8),
        ExcelData::Bool(_) => Ok(DataType::Boolean),
        ExcelData::DateTime(_) | ExcelData::DateTimeIso(_) => {
            Ok(DataType::Timestamp(TimeUnit::Second, None))
        }
        ExcelData::Error(e) => Err(Error::Load {
            msg: format!("{:?}", e),
        }),
        ExcelData::Empty => Ok(DataType::Null),
        _ => {
            if v.as_f64().is_some() {
                Ok(DataType::Float64)
            } else if v.as_i64().is_some() {
                Ok(DataType::Int64)
            } else if v.as_string().is_some() {
                Ok(DataType::Utf8)
            } else if v.get_bool().is_some() {
                Ok(DataType::Boolean)
            } else if v.as_datetime().is_some() {
                Ok(DataType::Timestamp(TimeUnit::Second, None))
            } else if v.as_duration().is_some() {
                Ok(DataType::Duration(TimeUnit::Second))
            } else {
                Err(Error::Load {
                    msg: format!("unsupported excel data type: {:?}", v),
                })
            }
        }
    }
}

fn infer_schema_from_data(mut range: ExcelSubrange) -> Result<Schema, Error> {
    let mut col_types: HashMap<&str, DataType> = HashMap::new();
    let col_names: Vec<&str> = range
        .next()
        .ok_or(Error::Load {
            msg: String::from("Failed to infer schema for empty excel table"),
        })?
        .iter()
        .enumerate()
        .map(|(i, c)| {
            c.get_string().ok_or_else(|| Error::Load {
                msg: format!("The {i}th column name is empty"),
            })
        })
        .collect::<Result<Vec<&str>, _>>()?;

    for row in range {
        for (i, col_val) in row.iter().enumerate() {
            let col_name = col_names.get(i).ok_or(Error::Load {
                msg: String::from(
                    "Failed to infer schema. Number of values in row is more then column names.",
                ),
            })?;
            let col_type = infer_value_type(col_val)?;
            col_types
                .entry(col_name)
                .and_modify(|ct| {
                    if !ct.equals_datatype(&col_type) && ct.equals_datatype(&DataType::Null) {
                        *ct = col_type.clone();
                    }
                    // if column values has more than one not null type then we upcast column type to the most general datatype Utf8.
                    else if !ct.equals_datatype(&col_type)
                        && !&col_type.equals_datatype(&DataType::Null)
                    {
                        *ct = DataType::Utf8;
                    }
                })
                .or_insert_with(|| col_type.clone());
        }
    }

    let fields: Vec<Field> = col_names
        .iter()
        .map(|col_name: &&str| {
            let dt = col_types.get(col_name).unwrap_or(&DataType::Utf8).clone();
            Field::new(col_name.replace(' ', "_"), dt, true)
        })
        .collect();
    Ok(Schema::new(fields))
}

fn infer_schema_from_config(table_schema: &TableSchema) -> Result<Schema, Error> {
    let unsupported_data_types = table_schema
        .columns
        .iter()
        .filter(|c| {
            !matches!(
                c.data_type,
                DataType::Boolean
                    | DataType::Int64
                    | DataType::Float64
                    | DataType::Duration(TimeUnit::Second)
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Null
                    | DataType::Utf8
                    | DataType::Timestamp(TimeUnit::Second, None)
            )
        })
        .map(|c| c.name.clone())
        .collect::<Vec<_>>()
        .join(", ");

    if unsupported_data_types.is_empty() {
        Ok(table_schema.into())
    } else {
        Err(Error::IncorrectSchema{msg: format!("Configured schema for excel file contains unsupported data types in columns {unsupported_data_types}. Supported datatype: \
            Boolean, Int64, Float64, Date32, Date64, !Timestamp [Second, null], !Duration [Second], Null, Utf8")})
    }
}

fn empty_or_panic<T>(v: &ExcelData, field_name: &String) -> Option<T> {
    if v.is_empty() {
        None
    } else {
        panic!("Incorrect value {v:?} in column {field_name}")
    }
}

fn infer_schema(
    r: &Range<ExcelData>,
    option: &TableOptionExcel,
    schema: &Option<TableSchema>,
) -> Result<Schema, Error> {
    let TableOptionExcel {
        rows_range_start,
        rows_range_end,
        columns_range_start,
        columns_range_end,
        schema_inference_lines,
        ..
    } = *option;

    if let Some(schema) = schema {
        infer_schema_from_config(schema)
    } else {
        let last_row_for_schema_inference = schema_inference_lines
            .map(|r| r + rows_range_start.unwrap_or(0))
            .or(rows_range_end);

        let range = ExcelSubrange::new(
            r,
            rows_range_start,
            last_row_for_schema_inference,
            columns_range_start,
            columns_range_end,
        );
        infer_schema_from_data(range)
    }
}

fn excel_range_to_record_batch(
    r: Range<ExcelData>,
    option: &TableOptionExcel,
    schema: Schema,
) -> Result<RecordBatch, Error> {
    let TableOptionExcel {
        rows_range_start,
        rows_range_end,
        columns_range_start,
        columns_range_end,
        ..
    } = *option;

    let arrays = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let rows = ExcelSubrange::new(
                &r,
                rows_range_start.map(|x| x + 1).or(Some(1)), // skip first row because it is header
                rows_range_end,
                columns_range_start,
                columns_range_end,
            );
            let field_name = field.name();

            match field.data_type() {
                DataType::Boolean => Arc::new(
                    rows.map(|r| {
                        r.get(i)
                            .and_then(|v| v.get_bool().or_else(|| empty_or_panic(v, field_name)))
                    })
                    .collect::<BooleanArray>(),
                ) as ArrayRef,
                DataType::Int64 => Arc::new(
                    rows.map(|r| {
                        r.get(i)
                            .and_then(|v| v.get_int().or_else(|| empty_or_panic(v, field_name)))
                    })
                    .collect::<PrimitiveArray<Int64Type>>(),
                ) as ArrayRef,
                DataType::Float64 => Arc::new(
                    rows.map(|r| {
                        r.get(i)
                            .and_then(|v| v.get_float().or_else(|| empty_or_panic(v, field_name)))
                    })
                    .collect::<PrimitiveArray<Float64Type>>(),
                ) as ArrayRef,
                DataType::Duration(TimeUnit::Second) => Arc::new(
                    rows.map(|r| {
                        r.get(i).and_then(|v| {
                            v.as_duration()
                                .map(|v: chrono::Duration| v.num_seconds())
                                .or_else(|| empty_or_panic(v, field_name))
                        })
                    })
                    .collect::<DurationSecondArray>(),
                ) as ArrayRef,
                DataType::Null => Arc::new(NullArray::new(rows.size())) as ArrayRef,
                DataType::Utf8 => Arc::new(
                    rows.map(|r| {
                        r.get(i).and_then(|v| match v {
                            ExcelData::Bool(x) => Some(x.to_string()),
                            ExcelData::Float(_) | ExcelData::Int(_) | ExcelData::String(_) => {
                                v.as_string()
                            }
                            ExcelData::DateTime(_) | ExcelData::DateTimeIso(_) => v
                                .as_datetime()
                                .map(|x: chrono::NaiveDateTime| x.to_string()),
                            _ if v.as_duration().is_some() => {
                                v.as_duration().map(|x: chrono::Duration| x.to_string())
                            }
                            ExcelData::Empty => None,
                            ExcelData::Error(e) => Some(format!("{:?}", e)),
                            _ => v.as_string(),
                        })
                    })
                    .collect::<StringArray>(),
                ) as ArrayRef,
                DataType::Timestamp(TimeUnit::Second, None) => Arc::new(
                    rows.map(|r| {
                        r.get(i).and_then(|v| {
                            v.as_datetime()
                                .map(|v: chrono::NaiveDateTime| v.and_utc().timestamp())
                                .or_else(|| empty_or_panic(v, field_name))
                        })
                    })
                    .collect::<TimestampSecondArray>(),
                ) as ArrayRef,
                DataType::Date64 => Arc::new(
                    rows.map(|r| {
                        r.get(i).and_then(|v| {
                            v.as_datetime()
                                .map(|v| v.and_utc().timestamp_millis())
                                .or_else(|| empty_or_panic(v, field_name))
                        })
                    })
                    .collect::<PrimitiveArray<Date64Type>>(),
                ) as ArrayRef,
                DataType::Date32 => Arc::new(
                    rows.map(|r| {
                        r.get(i).and_then(|v| {
                            v.as_datetime()
                                .map(|v| (v.and_utc().timestamp() / 86400) as i32)
                                .or_else(|| empty_or_panic(v, field_name))
                        })
                    })
                    .collect::<PrimitiveArray<Date32Type>>(),
                ) as ArrayRef,
                unsupported => panic!("Unsupported data type for excel table {unsupported:?}"),
            }
        })
        .collect::<Vec<ArrayRef>>();

    RecordBatch::try_new(Arc::new(schema), arrays).context(CreateRecordBatchSnafu)
}

pub async fn to_mem_table(
    t: &TableSource,
) -> Result<datafusion::datasource::MemTable, table::Error> {
    let opt = t
        .option
        .as_ref()
        .ok_or(table::Error::MissingOption {})?
        .as_excel()?;
    let uri = t.get_uri_str();
    let mut workbook: Sheets<_> = open_workbook_auto(uri)
        .context(OpenWorkbookSnafu)
        .map_err(Box::new)
        .context(table::LoadExcelSnafu)?;

    let worksheet_range = match &opt.sheet_name {
        Some(sheet) => Some(workbook.worksheet_range(sheet)),
        None => workbook.worksheet_range_at(0),
    };

    if let Some(Ok(range)) = worksheet_range {
        let shema = infer_schema(&range, opt, &t.schema)
            .map_err(Box::new)
            .context(table::LoadExcelSnafu)?;
        let batch = excel_range_to_record_batch(range, opt, shema)
            .map_err(Box::new)
            .context(table::LoadExcelSnafu)?;
        let schema_ref = batch.schema();
        let partitions = vec![vec![batch]];

        datafusion::datasource::MemTable::try_new(schema_ref, partitions)
            .map_err(Box::new)
            .context(table::CreateMemTableSnafu)
    } else {
        Err(Box::new(Error::Load {
            msg: "Failed to open excel file.".to_owned(),
        }))
        .context(table::LoadExcelSnafu)
    }
}

async fn to_datafusion_table(t: TableSource) -> Result<Arc<dyn TableProvider>, table::Error> {
    Ok(Arc::new(to_mem_table(&t).await?))
}

pub async fn to_loaded_table(t: TableSource) -> Result<LoadedTable, table::Error> {
    let reloader = Box::new(move || {
        Box::pin(to_datafusion_table(t.clone())) as crate::table::TableRefresherOutput
    });
    Ok(LoadedTable::new(reloader().await?, reloader))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{Float64Array, Int64Array};
    use crate::table::{TableColumn, TableIoSource};
    use crate::test_util::*;
    use datafusion::common::stats::Precision;
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;

    use calamine::{Cell, Data as ExcelData};

    #[test]
    fn excel_subrange_iteration() {
        let range = calamine::Range::<ExcelData>::from_sparse(vec![
            Cell::new((0, 0), ExcelData::Int(0)),
            Cell::new((0, 1), ExcelData::Bool(true)),
            Cell::new((0, 2), ExcelData::Float(0.333)),
            Cell::new((1, 0), ExcelData::Int(1)),
            Cell::new((1, 1), ExcelData::Bool(false)),
            Cell::new((1, 2), ExcelData::Float(1.333)),
            Cell::new((2, 0), ExcelData::Int(2)),
            Cell::new((2, 1), ExcelData::Empty),
            Cell::new((2, 2), ExcelData::Float(2.333)),
            Cell::new((3, 0), ExcelData::Int(3)),
            Cell::new((3, 1), ExcelData::Bool(true)),
            Cell::new((3, 2), ExcelData::Float(3.333)),
        ]);
        let mut subrange = ExcelSubrange::new(&range, None, None, None, None);
        assert_eq!(subrange.size(), 4);
        assert_eq!(
            subrange.next(),
            Some(
                &vec![
                    ExcelData::Int(0),
                    ExcelData::Bool(true),
                    ExcelData::Float(0.333)
                ][..]
            )
        );
        assert_eq!(
            subrange.next(),
            Some(
                &vec![
                    ExcelData::Int(1),
                    ExcelData::Bool(false),
                    ExcelData::Float(1.333)
                ][..]
            )
        );
        assert_eq!(
            subrange.next(),
            Some(&vec![ExcelData::Int(2), ExcelData::Empty, ExcelData::Float(2.333)][..])
        );
        assert_eq!(
            subrange.next(),
            Some(
                &vec![
                    ExcelData::Int(3),
                    ExcelData::Bool(true),
                    ExcelData::Float(3.333)
                ][..]
            )
        );
        assert_eq!(subrange.next(), None);

        let mut subrange = ExcelSubrange::new(&range, Some(1), Some(2), Some(1), Some(1));
        assert_eq!(subrange.size(), 2);
        assert_eq!(subrange.next(), Some(&vec![ExcelData::Bool(false)][..]));
        assert_eq!(subrange.next(), Some(&vec![ExcelData::Empty][..]));
        assert_eq!(subrange.next(), None);
    }

    #[test]
    fn inferes_schema_from_data() {
        let range = calamine::Range::<ExcelData>::from_sparse(vec![
            Cell::new((0, 0), ExcelData::String(String::from("int_column"))),
            Cell::new((0, 1), ExcelData::String(String::from("bool_column"))),
            Cell::new((0, 2), ExcelData::String(String::from("float column"))),
            Cell::new((0, 3), ExcelData::String(String::from("string_column"))),
            Cell::new((0, 4), ExcelData::String(String::from("datetime_column"))),
            Cell::new(
                (0, 5),
                ExcelData::String(String::from("datetime iso column")),
            ),
            Cell::new((0, 6), ExcelData::String(String::from("duration column"))),
            Cell::new(
                (0, 7),
                ExcelData::String(String::from("duration iso column")),
            ),
            Cell::new((1, 0), ExcelData::Int(0)),
            Cell::new((1, 1), ExcelData::Bool(true)),
            Cell::new((1, 2), ExcelData::Float(0.333)),
            Cell::new((1, 3), ExcelData::String(String::from("test"))),
            Cell::new(
                (1, 4),
                ExcelData::DateTime(calamine::ExcelDateTime::new(
                    44986.12,
                    calamine::ExcelDateTimeType::DateTime,
                    false,
                )),
            ),
            Cell::new((1, 5), ExcelData::DateTimeIso(String::from("test"))),
            Cell::new((1, 6), ExcelData::Float(44986.12)),
            Cell::new((1, 7), ExcelData::String(String::from("test"))),
            Cell::new((2, 0), ExcelData::Empty),
            Cell::new((2, 0), ExcelData::Empty),
            Cell::new((2, 1), ExcelData::Empty),
            Cell::new((2, 2), ExcelData::Empty),
            Cell::new((2, 3), ExcelData::Empty),
            Cell::new((2, 4), ExcelData::Empty),
            Cell::new((2, 5), ExcelData::Empty),
            Cell::new((2, 6), ExcelData::Empty),
            Cell::new((2, 7), ExcelData::Empty),
        ]);

        let schema = infer_schema(&range, &TableOptionExcel::default(), &None).unwrap();

        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("int_column", DataType::Int64, true),
                Field::new("bool_column", DataType::Boolean, true),
                Field::new("float_column", DataType::Float64, true),
                Field::new("string_column", DataType::Utf8, true),
                Field::new(
                    "datetime_column",
                    DataType::Timestamp(TimeUnit::Second, None),
                    true
                ),
                Field::new(
                    "datetime_iso_column",
                    DataType::Timestamp(TimeUnit::Second, None),
                    true
                ),
                Field::new("duration_column", DataType::Float64, true),
                Field::new("duration_iso_column", DataType::Utf8, true),
            ])
        );

        let range = calamine::Range::<ExcelData>::from_sparse(vec![
            Cell::new((0, 0), ExcelData::String(String::from("test_column"))),
            Cell::new((1, 0), ExcelData::Int(0)),
            Cell::new((2, 0), ExcelData::Empty),
            Cell::new((2, 0), ExcelData::Float(0.5)),
        ]);

        let schema = infer_schema(&range, &TableOptionExcel::default(), &None).unwrap();

        assert_eq!(
            schema,
            Schema::new(vec![Field::new("test_column", DataType::Utf8, true)])
        );

        let range = calamine::Range::<ExcelData>::from_sparse(vec![
            Cell::new((0, 0), ExcelData::String(String::from("int_column"))),
            Cell::new((0, 1), ExcelData::Empty),
            Cell::new((0, 2), ExcelData::String(String::from("float column"))),
        ]);

        assert!(infer_schema(&range, &TableOptionExcel::default(), &None).is_err());

        let range = calamine::Range::<ExcelData>::from_sparse(vec![
            Cell::new((0, 0), ExcelData::String(String::from("column1"))),
            Cell::new((0, 1), ExcelData::String(String::from("column2"))),
            Cell::new((1, 0), ExcelData::Int(1)),
            Cell::new((1, 1), ExcelData::Int(1)),
            Cell::new((1, 3), ExcelData::Int(1)),
        ]);
        assert!(infer_schema(&range, &TableOptionExcel::default(), &None).is_err());
    }

    #[test]
    fn inferes_schema_from_config() {
        let range = calamine::Range::<ExcelData>::from_sparse(vec![]);
        let table_schema = TableSchema {
            columns: vec![
                TableColumn {
                    name: String::from("float_column"),
                    data_type: DataType::Float64,
                    nullable: true,
                },
                TableColumn {
                    name: String::from("integer_column"),
                    data_type: DataType::Int64,
                    nullable: true,
                },
            ],
        };
        let schema =
            infer_schema(&range, &TableOptionExcel::default(), &Some(table_schema)).unwrap();

        assert_eq!(
            schema.flattened_fields(),
            vec![
                &Field::new("float_column", DataType::Float64, true),
                &Field::new("integer_column", DataType::Int64, true),
            ]
        );

        let table_schema = TableSchema {
            columns: vec![
                TableColumn {
                    name: String::from("float_column"),
                    data_type: DataType::Float16,
                    nullable: true,
                },
                TableColumn {
                    name: String::from("integer_column"),
                    data_type: DataType::Int16,
                    nullable: true,
                },
            ],
        };
        assert!(infer_schema(&range, &TableOptionExcel::default(), &Some(table_schema)).is_err());
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
            .scan(&ctx.state(), None, &[], None)
            .await
            .unwrap()
            .partition_statistics(None)
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(37));
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
            .scan(&ctx.state(), None, &[], None)
            .await
            .unwrap()
            .partition_statistics(None)
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(37));
    }

    #[tokio::test]
    async fn load_ods_with_custom_range_and_without_sheet_name() {
        let mut table_source: TableSource = serde_yaml::from_str(
            r#"
name: "test"
uri: "test_data/excel_range.ods"
option:
  format: "ods"
  rows_range_start: 2
  rows_range_end: 5
  columns_range_start: 1
  columns_range_end: 6
  schema_inference_lines: 3
"#,
        )
        .unwrap();
        // patch uri path with the correct test data path
        table_source.io_source = TableIoSource::Uri(test_data_path("excel_range.ods"));

        let t = to_mem_table(&table_source).await.unwrap();
        let ctx = SessionContext::new();
        let stats = t
            .scan(&ctx.state(), None, &[], None)
            .await
            .unwrap()
            .partition_statistics(None)
            .unwrap();
        assert_eq!(stats.column_statistics.len(), 6);
        assert_eq!(stats.num_rows, Precision::Exact(3));
    }

    #[test]
    fn transforms_excel_range_to_record_batch() {
        let range: calamine::Range<ExcelData> = calamine::Range::<ExcelData>::from_sparse(vec![
            Cell::new((0, 0), ExcelData::String("float_column".to_string())),
            Cell::new((1, 0), ExcelData::Float(1.333)),
            Cell::new((2, 0), ExcelData::Empty),
            Cell::new((3, 0), ExcelData::Float(3.333)),
            Cell::new((0, 1), ExcelData::String("integer_column".to_string())),
            Cell::new((1, 1), ExcelData::Int(1)),
            Cell::new((2, 1), ExcelData::Int(3)),
            Cell::new((3, 1), ExcelData::Empty),
            Cell::new((0, 2), ExcelData::String("boolean_column".to_string())),
            Cell::new((1, 2), ExcelData::Empty),
            Cell::new((2, 2), ExcelData::Bool(true)),
            Cell::new((3, 2), ExcelData::Bool(false)),
            Cell::new((0, 3), ExcelData::String("string_column".to_string())),
            Cell::new((1, 3), ExcelData::String("foo".to_string())),
            Cell::new((2, 3), ExcelData::String("bar".to_string())),
            Cell::new((3, 3), ExcelData::String("baz".to_string())),
            Cell::new((0, 4), ExcelData::String("mixed_column".to_string())),
            Cell::new((1, 4), ExcelData::Float(1.1)),
            Cell::new((2, 4), ExcelData::Int(1)),
            Cell::new((3, 4), ExcelData::Empty),
            Cell::new((0, 5), ExcelData::String("datetime_column".to_string())),
            Cell::new(
                (1, 5),
                ExcelData::DateTime(calamine::ExcelDateTime::new(
                    44986.12,
                    calamine::ExcelDateTimeType::DateTime,
                    false,
                )),
            ), // 2023-03-01T02:52:48
            Cell::new((2, 5), ExcelData::Empty),
            Cell::new(
                (3, 5),
                ExcelData::DateTime(calamine::ExcelDateTime::new(
                    44900.12,
                    calamine::ExcelDateTimeType::DateTime,
                    false,
                )),
            ), // 2022-12-05T02:52:48
        ]);

        let shema = infer_schema(&range, &TableOptionExcel::default(), &None).unwrap();
        let rb = excel_range_to_record_batch(range, &TableOptionExcel::default(), shema).unwrap();

        assert_eq!(
            rb.schema().flattened_fields(),
            vec![
                &Field::new("float_column", DataType::Float64, true),
                &Field::new("integer_column", DataType::Int64, true),
                &Field::new("boolean_column", DataType::Boolean, true),
                &Field::new("string_column", DataType::Utf8, true),
                &Field::new("mixed_column", DataType::Utf8, true),
                &Field::new(
                    "datetime_column",
                    DataType::Timestamp(TimeUnit::Second, None),
                    true
                ),
            ]
        );

        assert_eq!(
            rb.column(0).as_ref(),
            Arc::new(Float64Array::from(vec![Some(1.333), None, Some(3.333)])).as_ref(),
        );
        assert_eq!(
            rb.column(1).as_ref(),
            Arc::new(Int64Array::from(vec![Some(1), Some(3), None])).as_ref(),
        );
        assert_eq!(
            rb.column(2).as_ref(),
            Arc::new(BooleanArray::from(vec![None, Some(true), Some(false)])).as_ref(),
        );
        assert_eq!(
            rb.column(3).as_ref(),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])).as_ref(),
        );
        assert_eq!(
            rb.column(4).as_ref(),
            Arc::new(StringArray::from(vec![Some("1.1"), Some("1"), None])).as_ref(),
        );
        assert_eq!(
            rb.column(5).as_ref(),
            Arc::new(TimestampSecondArray::from(vec![
                Some(1677639168), // Unix timestamp for 2023-03-01T02:52:48 UTC
                None,
                Some(1670208768) // Unix timestamp for 2022-12-05T02:52:48 UTC
            ]))
            .as_ref(),
        );
    }
}
