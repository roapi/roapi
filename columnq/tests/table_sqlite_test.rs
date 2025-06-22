#[cfg(feature = "database-sqlite")]
mod sqlite {
    use columnq::table::TableSource;
    use columnq::ColumnQ;

    #[tokio::test]
    async fn text_column() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let conn = rusqlite::Connection::open(f.path()).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE users (name TEXT);
            INSERT INTO users VALUES ('Alice');
            INSERT INTO users VALUES ('Bob');
            ",
        )
        .unwrap();

        let mut cq = ColumnQ::new();

        cq.load_table(&TableSource::new(
            "users",
            format!("sqlite://{}", f.path().to_str().unwrap()),
        ))
        .await
        .unwrap();

        let batches = cq.query_sql("SELECT * FROM users").await.unwrap();

        assert_eq!(batches[0].num_rows(), 2);
    }
}
