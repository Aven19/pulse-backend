#!/bin/bash
# this script is used to boot a Docker container
retry_count=5
for i in $(seq $retry_count); do
    flask db upgrade
    if [[ "$?" == "0" ]]; then
        break
    fi
    echo Flask Deployment failed, retrying in 5 secs...
    sleep 5
done

exec /usr/bin/supervisord
