use crate::error::ColumnQError;
use crate::table::TableSource;
use datafusion::datasource::parquet::ParquetTable;

pub async fn to_mem_table(t: &TableSource) -> Result<ParquetTable, ColumnQError> {
    ParquetTable::try_new(t.parsed_uri()?, 4).map_err(|err| {
        ColumnQError::LoadParquet(format!("failed to load parquet: '{}'", err.to_string()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use datafusion::datasource::TableProvider;

    use crate::table::TableLoadOption;
    use crate::test_util::*;

    #[tokio::test]
    async fn load_simple_parquet() -> Result<(), ColumnQError> {
        let t = to_mem_table(&TableSource::new(
            "blogs".to_string(),
            test_data_path("blogs.parquet"),
        ))
        .await?;

        let schema = t.schema();
        assert_eq!(
            schema
                .metadata()
                .get("writer.model.name")
                .map(|s| s.as_str()),
            Some("protobuf")
        );

        assert_eq!(t.statistics().num_rows, Some(500));

        Ok(())
    }

    #[tokio::test]
    async fn load_partitions() -> anyhow::Result<()> {
        let tmp_dir = tempdir::TempDir::new("columnq.test.parquet_partitions")?;
        let tmp_dir_path = tmp_dir.path();

        let source_path = test_data_path("blogs.parquet");
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-01.parquet"))? > 0);
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-02.parquet"))? > 0);
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-03.parquet"))? > 0);

        let t = to_mem_table(
            &TableSource::new_with_uri("blogs", tmp_dir_path.to_string_lossy())
                .with_option(TableLoadOption::parquet {}),
        )
        .await?;

        assert_eq!(
            t.schema()
                .metadata()
                .get("writer.model.name")
                .map(|s| s.as_str()),
            Some("protobuf")
        );

        assert_eq!(t.statistics().num_rows, Some(1500));

        Ok(())
    }
}
