#!/bin/bash
http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT count(1) from s3_blogs" 127.0.0.1:8000/api/sql)

echo $http_status
if [[ $http_status != "200" ]]
then
    echo "error"
    exit 1
else
    echo "success"
fi

http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT count(1) from s3_blogs_dir" 127.0.0.1:8000/api/sql)

echo $http_status
if [[ $http_status != "200" ]]
then
    echo "error"
    exit 1
else
    echo "success"
fi

http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT count(1) from gcs_blogs" 127.0.0.1:8000/api/sql)

echo $http_status
if [[ $http_status != "200" ]]
then
    echo "error"
    exit 1
else
    echo "success"
fi

http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT count(1) from azure_blogs" 127.0.0.1:8000/api/sql)

echo $http_status
if [[ $http_status != "200" ]]
then
    echo "error"
    exit 1
else
    echo "success"
fi

exit 0
