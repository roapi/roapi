use anyhow::Result;
use roapi::config::Config;
use std::fs;

mod helpers;

#[test]
fn test_load_yaml_datafusion_config() -> Result<()> {
    let config_path = helpers::test_data_path("./test_datafusion_config.yml");
    let config_content = fs::read_to_string(config_path)?;

    let cfg: Config = serde_yaml::from_str(&config_content)?;
    let df_cfg = cfg.get_datafusion_config()?;

    assert_eq!(df_cfg.options().sql_parser.dialect, "Hive");
    assert_eq!(df_cfg.options().explain.physical_plan_only, true);
    assert_eq!(df_cfg.options().optimizer.max_passes, 10);
    assert_eq!(df_cfg.options().execution.batch_size, 100);
    assert_eq!(df_cfg.options().catalog.format, Some("parquet".to_string()));

    Ok(())
}
