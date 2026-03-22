#!/bin/bash

set -eux

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
TEST_DATA_DIR="${SCRIPT_DIR}/../../test_data"

# Use pinned az cli version via docker to ensure compatibility with Azurite
AZ_CLI_DOCKER="docker run --rm --network host -v ${TEST_DATA_DIR}:/test_data mcr.microsoft.com/azure-cli:2.60.0 az"

$AZ_CLI_DOCKER storage container create -n test-data --connection-string 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;QueueEndpoint=http://localhost:10001/devstoreaccount1;'
$AZ_CLI_DOCKER storage blob upload -f "/test_data/blogs.parquet" -c test-data -n blogs.parquet --connection-string 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;QueueEndpoint=http://localhost:10001/devstoreaccount1;'
