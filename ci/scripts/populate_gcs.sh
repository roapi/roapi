#!/bin/bash

set -eux

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

curl --fail-with-body -XPOST --data-binary '{"name":"test-data"}' -H "Content-Type: application/json" "http://localhost:4443/storage/v1/b"
curl --fail-with-body -XPOST --data-binary "@${SCRIPT_DIR}/../../test_data/blogs.parquet" "http://localhost:4443/upload/storage/v1/b/test-data/o?uploadType=media&name=blogs.parquet"
