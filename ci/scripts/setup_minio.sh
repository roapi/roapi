#!/bin/bash

set -eux

docker run -d -p 9000:9000 quay.io/minio/minio:RELEASE.2023-08-23T10-07-06Z server /data
