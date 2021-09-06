Columnq
=======

Simple CLI to help you query tabular data with support for a rich set of
growing formats and data sources.

It supports JSON, CSV, Parquet, Arrow and all other formats that are supported
by ROAPI, which is documented at
[here](https://roapi.github.io/docs/config/dataset-formats/index.html).

It also supports querying datasets from remote locations like S3 and HTTPs, see
ROAPI's [blob store
documentation](https://roapi.github.io/docs/config/blob-store.html) for more
info.


## Installation

### Pre-built binary

The pre-built binaries hosted on [GitHub releases](https://github.com/roapi/roapi/releases).
These binaries are self-contained so you can just drop them into your PATH.

The same set of binaries are also distributed through PyPI:

```bash
pip install columnq-cli
```

### Build from source

```bash
cargo install --locked --git https://github.com/roapi/roapi --branch main --bin columnq-cli
```


Usage
-----

### One off query

The `sql` sbucommand execute a provided SQL query against specificed static
dataset and return the result in stdout on exit. This is usually useful for
script automation tasks.

```
$ columnq sql --table test_data/spacex_launches.json \
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

By default, the `sql` subcommand outputs results in human friendly table
format. You can change the output format using `--output` option to make it
more friendly for automations.

```
$ columnq sql --table test_data/spacex_launches.json --output json "SELECT COUNT(id) AS total_launches FROM spacex_launches"
[{"total_launches":132}]
```

### Automate with UNIX pipes

Just like other UNIX tools, columnq supports consuming data stream from stdin
to integrate with other CLI tools using UNIX pipe:

```
find . -printf "%M|%n|%u|%s|%P\n" | columnq sql \
    --table 't=stdin,format=csv,has_header=false,delimiter=|' \
    "SELECT SUM(column_4) as total_size FROM t"
+------------+
| total_size |
+------------+
| 9875017987 |
+------------+
```

### Format conversion

The Columnq CLI can also be used as a handy utility to convert tabular data
between various formats: `json`, `parquet`, `csv`, `yaml`, `arrow`, etc.

```
$ columnq sql --table 't=test_data/uk_cities_with_headers.csv' 'SELECT * FROM t' --output json
$ cat test_data/blogs.parquet | columnq sql --table 't=stdin,format=parquet' 'SELECT * FROM t' --output json
```

### Interactive console

For dataset exploration, you can use the `console` subcommand to query multiple
datasets in an interactive console environment:

```
$ columnq console \
    --table "uk_cities=test_data/uk_cities_with_headers.csv" \
    --table "test_data/spacex_launches.json"
columnq(sql)> SELECT * FROM uk_cities WHERE lat > 57;
+-----------------------------+-----------+-----------+
| city                        | lat       | lng       |
+-----------------------------+-----------+-----------+
| Elgin, Scotland, the UK     | 57.653484 | -3.335724 |
| Aberdeen, Aberdeen City, UK | 57.149651 | -2.099075 |
| Inverness, the UK           | 57.477772 | -4.224721 |
+-----------------------------+-----------+-----------+
columnq(sql)> SELECT COUNT(*) FROM spacex_launches WHERE success=true AND upcoming=false;
+-----------------+
| COUNT(UInt8(1)) |
+-----------------+
| 111             |
+-----------------+
```

Explore in memory catalog and table schemas:

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

Development
-----------

### Debug mode

Set the `RUST_LOG` environment variable to `info,columnq=debug` to run columnq
in verbose debug logging.
