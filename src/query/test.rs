use std::sync::Arc;

use datafusion::dataframe::DataFrame;

pub fn assert_eq_df(df1: Arc<dyn DataFrame>, df2: Arc<dyn DataFrame>) {
    assert_eq!(
        format!("{:?}", df1.to_logical_plan()),
        format!("{:?}", df2.to_logical_plan())
    );
}
