use std::path::PathBuf;

pub fn test_data_path(relative_path: &str) -> String {
    let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("../test_data");
    d.push(relative_path);
    d.to_string_lossy().to_string()
}
