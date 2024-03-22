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

http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT count(1) from s3_blogs" "${SQL_ENDPOINT}")
check_status ${http_status}

http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT count(1) from s3_blogs_space_encode" "${SQL_ENDPOINT}")
check_status ${http_status}

http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT count(1) from s3_blogs_dir" "${SQL_ENDPOINT}")
check_status ${http_status}

http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT count(1) from gcs_blogs" "${SQL_ENDPOINT}")
check_status ${http_status}

http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT count(1) from azure_blogs" "${SQL_ENDPOINT}")
check_status ${http_status}
