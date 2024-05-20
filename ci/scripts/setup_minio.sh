#!/bin/bash

set -eux

CTLBIN="docker"
if which podman &>/dev/null; then
    CTLBIN="podman"
fi

${CTLBIN} run -d --name minio -p 9000:9000 quay.io/minio/minio:RELEASE.2023-08-23T10-07-06Z server /data
