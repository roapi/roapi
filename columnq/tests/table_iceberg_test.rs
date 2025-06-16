use columnq::table::{
    iceberg::to_loaded_table, 
    TableLoadOption, 
    TableOptionIceberg, 
    TableSource
};

#[tokio::test]
async fn test_iceberg_empty_table_current_implementation() {
    // Test current implementation which creates empty table as mentioned by @houqp
    let table_source = TableSource::new(
        "test_table", 
        "iceberg://localhost:8181/test_namespace/test_table"
    ).with_option(TableLoadOption::iceberg(TableOptionIceberg {
        use_memory_table: true,
    }));

    // This test demonstrates the current behavior where we get an empty table
    // The test will fail because there's no actual iceberg server running,
    // but it shows the URI parsing and table creation logic
    let result = to_loaded_table(&table_source).await;
    
    // We expect this to fail due to connection error (no server running)
    // but the error should be about connection, not URI parsing
    match result {
        Err(e) => {
            let error_msg = e.to_string();
            println!("Expected connection error: {}", error_msg);
            // The error should be about HTTP connection, not URI parsing
            assert!(!error_msg.contains("Invalid URI") && !error_msg.contains("URI must have"));
        }
        Ok(_) => panic!("Expected connection error but got success"),
    }
}

#[tokio::test]
async fn test_iceberg_invalid_uri_format() {
    // Test with invalid URI format (missing namespace/table)
    let table_source = TableSource::new(
        "test_table", 
        "iceberg://localhost:8181/only_namespace"
    ).with_option(TableLoadOption::iceberg(TableOptionIceberg {
        use_memory_table: true,
    }));

    let result = to_loaded_table(&table_source).await;
    
    match result {
        Err(e) => {
            let error_msg = e.to_string();
            println!("Expected URI format error: {}", error_msg);
            assert!(error_msg.contains("must be in format iceberg://host"));
        }
        Ok(_) => panic!("Expected URI format error but got success"),
    }
}

#[tokio::test]
async fn test_iceberg_malformed_uri() {
    // Test with completely malformed URI
    let table_source = TableSource::new(
        "test_table", 
        "not-a-valid-uri"
    ).with_option(TableLoadOption::iceberg(TableOptionIceberg {
        use_memory_table: true,
    }));

    let result = to_loaded_table(&table_source).await;
    
    match result {
        Err(e) => {
            let error_msg = e.to_string();
            println!("Expected malformed URI error: {}", error_msg);
            // Should fail due to invalid URI or missing scheme
            assert!(error_msg.contains("Failed to parse URI") || error_msg.contains("URI must have"));
        }
        Ok(_) => panic!("Expected malformed URI error but got success"),
    }
}

#[tokio::test]
async fn test_iceberg_non_memory_table_option() {
    // Test with use_memory_table: false
    let table_source = TableSource::new(
        "test_table", 
        "iceberg://localhost:8181/test_namespace/test_table"
    ).with_option(TableLoadOption::iceberg(TableOptionIceberg {
        use_memory_table: false,
    }));

    let result = to_loaded_table(&table_source).await;
    
    // This should also fail with connection error since we don't have a real server
    match result {
        Err(e) => {
            let error_msg = e.to_string();
            println!("Expected connection error with non-memory table: {}", error_msg);
            // The error should be about HTTP connection, not URI parsing
            assert!(!error_msg.contains("Invalid URI") && !error_msg.contains("URI must have"));
        }
        Ok(_) => panic!("Expected connection error but got success"),
    }
}

#[tokio::test] 
async fn test_iceberg_non_iceberg_scheme() {
    // Test with non-iceberg scheme should be rejected
    let table_source = TableSource::new(
        "test_table", 
        "https://localhost:8181/test_namespace/test_table"
    ).with_option(TableLoadOption::iceberg(TableOptionIceberg {
        use_memory_table: true,
    }));

    let result = to_loaded_table(&table_source).await;
    
    match result {
        Err(e) => {
            let error_msg = e.to_string();
            println!("Expected scheme error: {}", error_msg);
            assert!(error_msg.contains("Only iceberg://"));
        }
        Ok(_) => panic!("Expected scheme error but got success"),
    }
} 