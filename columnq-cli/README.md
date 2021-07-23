Columnq
=======

Easy to use library and CLI to help you query tabular data with support for a
rich set of growing formats data sources.


Usage
-----

Show schemas for a specific table:

```
columnq(sql)> show columns from uk_cities;
+---------------+--------------+------------+-------------+-----------+-------------+
| table_catalog | table_schema | table_name | column_name | data_type | is_nullable |
+---------------+--------------+------------+-------------+-----------+-------------+
| datafusion    | public       | uk_cities  | city        | Utf8      | NO          |
| datafusion    | public       | uk_cities  | lat         | Float64   | NO          |
| datafusion    | public       | uk_cities  | lng         | Float64   | NO          |
+---------------+--------------+------------+-------------+-----------+-------------+
```

The Columnq CLI can also be used as a handy utility to Convert tabular data
between various formats: `json`, `parquet`, `csv`, `yaml`, `arrow`, etc.

```
$ columnq sql --table 't=test_data/uk_cities_with_headers.csv' 'SELECT * FROM t' --output json
$ cat test_data/blogs.parquet | columnq sql --table 't=stdin,format=parquet' 'SELECT * FROM t' --output json
```
