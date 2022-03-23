#!/bin/bash

ES_HOST=${1:-localhost}

echo "Using the following command for ES health-check:"
echo "curl --request GET -sL \
         --url http://$ES_HOST:9200/_cluster/health \
         --output /dev/null"

i=1

while [[ $i -ne 0 ]] ;
do
    curl --request GET --url "http://$ES_HOST:9200/_cluster/health"
    i=$?
done

sbt integration:test || exit 1
