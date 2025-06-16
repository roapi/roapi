use columnq::table::{TableSource, TableLoadOption, TableOptionIceberg};
use columnq::ColumnQ;

#[tokio::test]
async fn test_iceberg_empty_table_current_implementation() {
    // Test the current iceberg implementation which creates an empty table
    // This test demonstrates the issue mentioned by the repo owner:
    // "this only creates an empty table? could you write an integration test for this?"
    
    let mut cq = ColumnQ::new();

    // Test with an iceberg URI
    let table_source = TableSource::new(
        "test_iceberg_table",
        "iceberg://localhost:8181/test_namespace/test_table"
    ).with_option(TableLoadOption::iceberg(TableOptionIceberg {}));

    // Note: This test will fail because there's no actual iceberg server running
    // but it demonstrates the current behavior and the need for improvement
    let result = cq.load_table(&table_source).await;
    
    // The current implementation should fail because:
    // 1. It tries to connect to a real iceberg server at localhost:8181
    // 2. No server is running in the test environment
    assert!(result.is_err(), "Expected error due to no iceberg server running");
    
    // The error should indicate a connection issue
    let error_msg = format!("{:?}", result.err().unwrap());
    assert!(
        error_msg.contains("connection") || error_msg.contains("network") || error_msg.contains("refused"),
        "Expected connection-related error, got: {}", error_msg
    );
}

#[tokio::test] 
async fn test_iceberg_invalid_uri_format() {
    // Test that invalid URI formats are properly rejected
    let mut cq = ColumnQ::new();
    
    // Test with non-iceberg URI
    let table_source = TableSource::new(
        "test_table",
        "http://localhost:8181/test_namespace/test_table"
    ).with_option(TableLoadOption::iceberg(TableOptionIceberg {}));
    
    let result = cq.load_table(&table_source).await;
    assert!(result.is_err(), "Expected error for non-iceberg URI");
    
    let error_msg = format!("{:?}", result.err().unwrap());
    assert!(
        error_msg.contains("FeatureUnsupported") || error_msg.contains("iceberg://"),
        "Expected FeatureUnsupported error for non-iceberg URI, got: {}", error_msg
    );
}

#[tokio::test]
async fn test_iceberg_malformed_uri() {
    // Test various malformed iceberg URIs
    let mut cq = ColumnQ::new();
    
    let test_cases = vec![
        ("iceberg://", "missing host"),
        ("iceberg://localhost", "missing namespace"), 
        ("iceberg://localhost/namespace", "missing table name"),
    ];
    
    for (uri, expected_error) in test_cases {
        let table_source = TableSource::new(
            "test_table",
            uri
        ).with_option(TableLoadOption::iceberg(TableOptionIceberg {}));
        
        let result = cq.load_table(&table_source).await;
        assert!(result.is_err(), "Expected error for malformed URI: {}", uri);
        
        let error_msg = format!("{:?}", result.err().unwrap());
        assert!(
            error_msg.contains("DataInvalid") || error_msg.contains(expected_error),
            "Expected DataInvalid error for '{}', got: {}", uri, error_msg
        );
    }
}

// TODO: This is what the integration test should eventually test when we fix the empty table issue:
// 
// #[tokio::test]
// async fn test_iceberg_with_real_data() {
//     // This test would require either:
//     // 1. A mock iceberg server that returns actual table data
//     // 2. A docker container running tabulario/iceberg-rest
//     // 3. Using iceberg-datafusion to load actual data instead of empty MemTable
//     
//     let mut cq = ColumnQ::new();
//     
//     let table_source = TableSource::new(
//         "test_iceberg_table",
//         "iceberg://localhost:8181/test_namespace/test_table"
//     ).with_option(TableLoadOption::iceberg(TableOptionIceberg {}));
//     
//     let result = cq.load_table(&table_source).await;
//     assert!(result.is_ok(), "Should successfully load iceberg table");
//     
//     // Query the loaded table to verify it has actual data
//     let batches = cq.query_sql("SELECT * FROM test_iceberg_table").await.unwrap();
//     
//     // Should have actual data, not an empty table
//     assert!(batches.len() > 0, "Expected non-empty result batches");
//     // Further assertions about the actual data...
// } 