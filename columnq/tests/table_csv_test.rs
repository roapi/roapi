mod helpers;

use std::sync::Arc;

use datafusion::arrow;
use datafusion::prelude::SessionContext;

use columnq::table::csv::to_datafusion_table;
use columnq::table::{TableIoSource, TableLoadOption, TableOptionCsv, TableSource};

#[tokio::test]
async fn infer_csv_schema_by_selected_files() {
    use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder};

    let table_path = helpers::test_data_path("partitioned_csv");

    let ctx = SessionContext::new();

    let table_io_source = TableIoSource::Uri(table_path);
    let table_source = TableSource::new("test", table_io_source)
        .with_schema_from_files(vec![])
        .with_option(TableLoadOption::csv(
            TableOptionCsv::default().with_use_memory_table(false),
        ));
    assert!(
        !table_source
            .option
            .as_ref()
            .unwrap()
            .as_csv()
            .unwrap()
            .use_memory_table
    );
    assert!(table_source.schema_from_files.is_some());
    assert_eq!(table_source.schema, None);

    match to_datafusion_table(&table_source, &ctx).await {
        Err(columnq::table::Error::Generic { msg }) => {
            assert_eq!(&msg, "schema_from_files is an empty list");
        }
        _ => panic!("Empty schema_from_files should result in an error"),
    }

    let t = to_datafusion_table(
        &table_source.with_schema_from_files(vec!["year=2023/month=1/p001.csv".to_string()]),
        &ctx,
    )
    .await
    .unwrap();

    let mut builder = SchemaBuilder::new();
    builder.push(Field::new("ts", DataType::Int64, true));
    builder.push(Field::new("value", DataType::Float64, true));

    assert_eq!(
        t.table.schema(),
        Arc::new(Schema::new(builder.finish().fields))
    );
}
