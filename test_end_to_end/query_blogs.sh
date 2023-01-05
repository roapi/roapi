#!/bin/sh
http_status=$(curl -o /dev/null -s -w "%{http_code}" -X POST -d "SELECT count(1) from blogs" 127.0.0.1:8000/api/sql)

echo $http_status
if [[ $http_status != "200" ]]
then
    # error
    exit 1
else
    # success
    exit 0
fi
