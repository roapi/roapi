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
