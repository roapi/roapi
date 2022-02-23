use datafusion::arrow;

use crate::error::ColumnQError;

pub fn record_batches_to_bytes(
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<Vec<u8>, ColumnQError> {
    let json_rows = arrow::json::writer::record_batches_to_json_rows(batches)?;
    serde_json::to_vec(&json_rows).map_err(ColumnQError::json_parse)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::*;
    use datafusion::arrow::record_batch::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn serialize_date_columns() {
        let schema = Schema::new(vec![
            Field::new("d32", DataType::Date32, false),
            Field::new("d64", DataType::Date64, false),
        ]);

        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Date32Array::from(vec![1, 18729])),
                Arc::new(Date64Array::from(vec![1, 1618200268000])),
            ],
        )
        .unwrap();

        let data = record_batches_to_bytes(&[record_batch]).unwrap();

        assert_eq!(
            std::str::from_utf8(&data).unwrap(),
            serde_json::json!([
                {
                    "d32": "1970-01-02",
                    "d64": "1970-01-01",
                },
                {
                    "d32": "2021-04-12",
                    "d64": "2021-04-12",
                },
            ])
            .to_string(),
        )
    }

    #[test]
    fn serialize_timestamp_columns() {
        let schema = Schema::new(vec![
            Field::new("sec", DataType::Timestamp(TimeUnit::Second, None), false),
            Field::new(
                "msec",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "usec",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "nsec",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]);

        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(TimestampSecondArray::from(vec![1618200268, 1620792268])),
                Arc::new(TimestampMillisecondArray::from(vec![
                    1618200268000,
                    1620792268001,
                ])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1618200268000000,
                    1620792268000002,
                ])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1618200268000000000,
                    1620792268000000003,
                ])),
            ],
        )
        .unwrap();

        let data = record_batches_to_bytes(&[record_batch]).unwrap();

        assert_eq!(
            std::str::from_utf8(&data).unwrap(),
            serde_json::json!([
                {
                    "sec": "2021-04-12 04:04:28",
                    "msec": "2021-04-12 04:04:28",
                    "usec": "2021-04-12 04:04:28",
                    "nsec": "2021-04-12 04:04:28",
                },
                {
                    "sec": "2021-05-12 04:04:28",
                    "msec": "2021-05-12 04:04:28.001",
                    "usec": "2021-05-12 04:04:28.000002",
                    "nsec": "2021-05-12 04:04:28.000000003",
                }
            ])
            .to_string(),
        )
    }

    #[test]
    fn serialize_time_columns() {
        let schema = Schema::new(vec![
            Field::new("t32sec", DataType::Time32(TimeUnit::Second), false),
            Field::new("t32msec", DataType::Time32(TimeUnit::Millisecond), false),
            Field::new("t64usec", DataType::Time64(TimeUnit::Microsecond), false),
            Field::new("t64nsec", DataType::Time64(TimeUnit::Nanosecond), false),
        ]);

        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Time32SecondArray::from(vec![1, 120])),
                Arc::new(Time32MillisecondArray::from(vec![1, 120])),
                Arc::new(Time64MicrosecondArray::from(vec![1, 120])),
                Arc::new(Time64NanosecondArray::from(vec![1, 120])),
            ],
        )
        .unwrap();

        let data = record_batches_to_bytes(&[record_batch]).unwrap();

        assert_eq!(
            std::str::from_utf8(&data).unwrap(),
            serde_json::json!([
                {
                    "t32sec": "00:00:01",
                    "t32msec": "00:00:00.001",
                    "t64usec": "00:00:00.000001",
                    "t64nsec": "00:00:00.000000001",
                },
                {
                    "t32sec": "00:02:00",
                    "t32msec": "00:00:00.120",
                    "t64usec": "00:00:00.000120",
                    "t64nsec": "00:00:00.000000120",
                },
            ])
            .to_string()
        )
    }
}
