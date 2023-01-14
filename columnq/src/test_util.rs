use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;

use crate::table;

pub fn test_data_path(relative_path: &str) -> String {
    let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("../test_data");
    d.push(relative_path);
    d.to_string_lossy().to_string()
}

fn properties_table() -> anyhow::Result<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("address", DataType::Utf8, false),
        Field::new("landlord", DataType::Utf8, false),
        Field::new("bed", DataType::Int64, false),
        Field::new("bath", DataType::Int64, false),
        Field::new("occupied", DataType::Boolean, false),
        Field::new("monthly_rent", DataType::Utf8, false),
        Field::new("lease_expiration_date", DataType::Utf8, false),
    ]));

    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "Bothell, WA",
                "Lynnwood, WA",
                "Kirkland, WA",
                "Kent, WA",
                "Mount Vernon, WA",
                "Seattle, WA",
                "Seattle, WA",
                "Shoreline, WA",
                "Bellevue, WA",
                "Renton, WA",
                "Woodinville, WA",
                "Kenmore, WA",
                "Fremont, WA",
                "Redmond, WA",
                "Mill Creek, WA",
            ])),
            Arc::new(StringArray::from(vec![
                "Roger", "Daniel", "Mike", "Mike", "Roger", "Carl", "Daniel", "Roger", "Mike",
                "Carl", "Carl", "Sam", "Daniel", "Mike", "Sam",
            ])),
            Arc::new(Int64Array::from(vec![
                3, 2, 4, 3, 2, 3, 2, 1, 3, 4, 3, 4, 5, 2, 3,
            ])),
            Arc::new(Int64Array::from(vec![
                2, 1, 2, 2, 1, 1, 1, 1, 1, 2, 3, 3, 3, 2, 3,
            ])),
            Arc::new(BooleanArray::from(vec![
                false, false, true, false, false, true, true, true, false, true, false, false,
                false, false, true,
            ])),
            Arc::new(StringArray::from(vec![
                "$2,000", "$1,700", "$3,000", "$3,800", "$1,500", "$3,000", "$1,500", "$1,200",
                "$2,400", "$2,800", "$3,000", "$4,000", "$4,500", "$2,200", "$3,500",
            ])),
            Arc::new(StringArray::from(vec![
                "10/23/2020",
                "6/10/2019",
                "6/24/2021",
                "10/31/2020",
                "11/5/2019",
                "12/28/2021",
                "4/29/2021",
                "12/9/2021",
                "2/15/2020",
                "10/22/2021",
                "5/30/2019",
                "9/22/2019",
                "7/13/2019",
                "5/31/2020",
                "8/4/2021",
            ])),
        ],
    )?;

    Ok(MemTable::try_new(schema, vec![vec![record_batch]])?)
}

async fn ubuntu_ami_table() -> anyhow::Result<Arc<dyn datafusion::datasource::TableProvider>> {
    let mut table_source: table::TableSource = serde_yaml::from_str(
        r#"
name: "ubuntu_ami"
uri: "test_data/ubuntu-ami.json"
option:
  format: "json"
  pointer: "/aaData"
  array_encoded: true
schema:
  columns:
    - name: "zone"
      data_type: "Utf8"
    - name: "name"
      data_type: "Utf8"
    - name: "version"
      data_type: "Utf8"
    - name: "arch"
      data_type: "Utf8"
    - name: "instance_type"
      data_type: "Utf8"
    - name: "release"
      data_type: "Utf8"
    - name: "ami_id"
      data_type: "Utf8"
    - name: "aki_id"
      data_type: "Utf8"
"#,
    )?;

    // patch uri path with the correct test data path
    table_source.io_source = table::TableIoSource::Uri(test_data_path("ubuntu-ami.json"));
    let ctx = SessionContext::new();
    Ok(table::load(&table_source, &ctx).await?)
}

pub fn register_table_properties(dfctx: &mut SessionContext) -> anyhow::Result<()> {
    dfctx.register_table("properties", Arc::new(properties_table()?))?;
    Ok(())
}

pub async fn register_table_ubuntu_ami(dfctx: &mut SessionContext) -> anyhow::Result<()> {
    dfctx.register_table("ubuntu_ami", ubuntu_ami_table().await?)?;
    Ok(())
}

pub fn assert_eq_df(df1: Arc<DataFrame>, df2: Arc<DataFrame>) {
    assert_eq!(
        format!("{:?}", df1.to_logical_plan()),
        format!("{:?}", df2.to_logical_plan())
    );
}
