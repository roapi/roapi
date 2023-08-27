#!/bin/bash

set -eux

docker run -d -p 4443:4443 fsouza/fake-gcs-server -scheme http
echo '{"gcs_base_url": "http://localhost:4443", "disable_oauth": true, "client_email": "", "private_key": ""}' > "/tmp/gcs.json"
