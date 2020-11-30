serialization
  - [x] JSON `application/json`
  - [x] Arrow `application/vnd.apache.arrow.stream`
  - [ ] msgpack

query layer:
  - [x] REST API GET
  - [x] GraphQL
  - [x] SQL

meta api:
  - [x] schema

data format:
  - [x] CSV
  - [x] JSON
  - [x] parquet
  - [ ] Partitions
  - [ ] xls, xlsx, xlsm, ods: https://github.com/tafia/calamine

storage layer:
  - [x] filesystem
  - [x] HTTP/HTTPS
  - [ ] S3
  - [x] Google spreadsheet
  - [ ] Confluence Wiki Table
  - [ ] MySQL
  - [ ] Postgres

misc
  - [ ] query input type conversion based on table schema
  - [x] error handling in API
  - [x] separate query and api layer
  - [ ] stream arrow encoding response
