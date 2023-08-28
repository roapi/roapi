#!/bin/bash

set -eux

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

curl --fail-with-body https://dl.min.io/client/mc/release/linux-amd64/mc --create-dirs -o $HOME/minio-binaries/mc
chmod +x $HOME/minio-binaries/mc

TEST_DATA_DIR="${SCRIPT_DIR}/../../test_data"

$HOME/minio-binaries/mc alias set local http://127.0.0.1:9000 minioadmin minioadmin
$HOME/minio-binaries/mc mb local/test-data
$HOME/minio-binaries/mc cp "${TEST_DATA_DIR}"/blogs.parquet local/test-data
$HOME/minio-binaries/mc cp "${TEST_DATA_DIR}"/blogs.parquet "local/test-data/blogs space.parquet"
$HOME/minio-binaries/mc cp "${TEST_DATA_DIR}"/blogs.parquet local/test-data/blogs/
