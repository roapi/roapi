#!/bin/bash
set -eu

function check_status() {
    http_status=$1
    echo $http_status
    if [[ $http_status != "200" ]]
    then
        echo "error"
        exit 1
    else
        echo "success"
    fi
}

SQL_ENDPOINT="127.0.0.1:8000/api/sql"

echo "Test s3 blogs..."
http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT count(1) from s3_blogs" "${SQL_ENDPOINT}")
check_status ${http_status}

echo "Test s3 blogs space encoded..."
http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT count(1) from s3_blogs_space_encode" "${SQL_ENDPOINT}")
check_status ${http_status}

echo "Test s3 blogs in a directory..."
http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT count(1) from s3_blogs_dir" "${SQL_ENDPOINT}")
check_status ${http_status}

# in memory table doesn't yet support populating partitions from object path, so we skip the partition query here if we are not testing direct tables.
if "${DIRECT_TABLE:=false}" = "true"; then
    echo "Test s3 blogs as a partitioned table..."
    http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT year, month from s3_partitioned" "${SQL_ENDPOINT}")
    check_status ${http_status}
fi

http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT count(*) from s3_delta WHERE reply_id is null" "${SQL_ENDPOINT}")
check_status ${http_status}

echo "Test gcs blogs..."
http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT count(1) from gcs_blogs" "${SQL_ENDPOINT}")
check_status ${http_status}

echo "Test azure blogs..."
http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT count(1) from azure_blogs" "${SQL_ENDPOINT}")
check_status ${http_status}
