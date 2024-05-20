#!/bin/bash

set -eux

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
TEST_DATA_DIR="${SCRIPT_DIR}/../../test_data"

MCBIN=mc
if ! which "${MCBIN}" &>/dev/null; then
    curl --fail-with-body https://dl.min.io/client/mc/release/linux-amd64/mc --create-dirs -o $HOME/minio-binaries/mc
    chmod +x $HOME/minio-binaries/mc
    MCBIN=$HOME/minio-binaries/mc
fi

"${MCBIN}" alias set local http://127.0.0.1:9000 minioadmin minioadmin
"${MCBIN}" mb local/test-data

"${MCBIN}" cp "${TEST_DATA_DIR}"/blogs.parquet local/test-data
"${MCBIN}" cp "${TEST_DATA_DIR}"/blogs.parquet "local/test-data/blogs space.parquet"
"${MCBIN}" cp "${TEST_DATA_DIR}"/blogs.parquet local/test-data/blogs/
# populate partitioned table in S3
"${MCBIN}" cp "${TEST_DATA_DIR}"/blogs.parquet local/test-data/partitioned_blogs/year=2024/month=10/
"${MCBIN}" cp "${TEST_DATA_DIR}"/blogs.parquet local/test-data/partitioned_blogs/year=2023/month=2/
# populate delta table
"${MCBIN}" cp --recursive "${TEST_DATA_DIR}"/blogs-delta local/test-data/
