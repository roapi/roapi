Columnq
=======

Easy to use library and CLI to help you query tabular data with support for a
rich set of growing formats data sources.


Usage
-----

### Query once

```
columnq sql --table test_data/spacex_launches.json \
  "SELECT COUNT(id), DATE_TRUNC('year', CAST(date_utc AS TIMESTAMP)) as d FROM spacex_launches WHERE success = true GROUP BY d ORDER BY d DESC"
+-----------+---------------------+
| COUNT(id) | d                   |
+-----------+---------------------+
| 4         | 2021-01-01 00:00:00 |
| 26        | 2020-01-01 00:00:00 |
| 13        | 2019-01-01 00:00:00 |
| 21        | 2018-01-01 00:00:00 |
| 18        | 2017-01-01 00:00:00 |
| 8         | 2016-01-01 00:00:00 |
| 6         | 2015-01-01 00:00:00 |
| 6         | 2014-01-01 00:00:00 |
| 3         | 2013-01-01 00:00:00 |
| 2         | 2012-01-01 00:00:00 |
| 2         | 2010-01-01 00:00:00 |
| 1         | 2009-01-01 00:00:00 |
| 1         | 2008-01-01 00:00:00 |
+-----------+---------------------+
```

The Columnq CLI can also be used as a handy utility to Convert tabular data
between various formats: `json`, `parquet`, `csv`, `yaml`, `arrow`, etc.

```
$ columnq sql --table 't=test_data/uk_cities_with_headers.csv' 'SELECT * FROM t' --output json
$ cat test_data/blogs.parquet | columnq sql --table 't=stdin,format=parquet' 'SELECT * FROM t' --output json
```

### Interactive console

Query multiple datasets in an interactive console:

```
columnq(sql)> SELECT * FROM uk_cities WHERE lat > 57;
+-----------------------------+-----------+-----------+
| city                        | lat       | lng       |
+-----------------------------+-----------+-----------+
| Elgin, Scotland, the UK     | 57.653484 | -3.335724 |
| Aberdeen, Aberdeen City, UK | 57.149651 | -2.099075 |
| Inverness, the UK           | 57.477772 | -4.224721 |
+-----------------------------+-----------+-----------+
```

Show schemas for a specific table:

```
columnq(sql)> SHOW TABLES;
+---------------+--------------------+-----------------+------------+
| table_catalog | table_schema       | table_name      | table_type |
+---------------+--------------------+-----------------+------------+
| datafusion    | public             | uk_cities       | BASE TABLE |
| datafusion    | public             | spacex_launches | BASE TABLE |
| datafusion    | information_schema | tables          | VIEW       |
| datafusion    | information_schema | columns         | VIEW       |
+---------------+--------------------+-----------------+------------+
columnq(sql)> SHOW COLUMNS FROM uk_cities;
+---------------+--------------+------------+-------------+-----------+-------------+
| table_catalog | table_schema | table_name | column_name | data_type | is_nullable |
+---------------+--------------+------------+-------------+-----------+-------------+
| datafusion    | public       | uk_cities  | city        | Utf8      | NO          |
| datafusion    | public       | uk_cities  | lat         | Float64   | NO          |
| datafusion    | public       | uk_cities  | lng         | Float64   | NO          |
+---------------+--------------+------------+-------------+-----------+-------------+
```
