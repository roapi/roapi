use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext;

fn properties_table() -> anyhow::Result<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("Address", DataType::Utf8, false),
        Field::new("Landlord", DataType::Utf8, false),
        Field::new("Bed", DataType::Int64, false),
        Field::new("Bath", DataType::Int64, false),
        Field::new("Occupied", DataType::Boolean, false),
        Field::new("Monthly_Rent", DataType::Utf8, false),
        Field::new("Lease_Expiration_Date", DataType::Utf8, false),
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

pub fn register_table_properties(dfctx: &mut ExecutionContext) -> anyhow::Result<()> {
    dfctx.register_table("properties", Box::new(properties_table()?));
    Ok(())
}
