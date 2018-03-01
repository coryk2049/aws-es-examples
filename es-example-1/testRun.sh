#!/bin/bash

ES_ENDPOINT=<MY-ES-END-POINT>.us-east-1.es.amazonaws.com
ES_URL=https://${ES_ENDPOINT}/bl_edr
TEST_CSV_FILE=pcrf1.example.com_edr_B25_20170101000000_20170101000014.csv

info() {
  echo -e "[`date '+%m/%d/%Y-%H:%M:%S'`]::INFO::---------------------------------------------------"
	echo -e "[`date '+%m/%d/%Y-%H:%M:%S'`]::INFO::$1"
  echo -e "[`date '+%m/%d/%Y-%H:%M:%S'`]::INFO::---------------------------------------------------"
}

cd ~ ; source dot.profile

info "Remove previous ES loader run log"
rm -f esLoader_*.log

info "Delete index if it exists"
curl -XDELETE ${ES_URL}/

info "Load some data into ES"
./esLoader.py -b 100 -s 2 -l INFO \
  -f ${TEST_CSV_FILE} \
  -e ${ES_ENDPOINT}

#info "Display ES loader run log"
#cat esLoader_*.log

info "Sample index data query"
curl -k -XGET ${ES_URL}/_search?pretty

info "Sample index total record query"
curl -k -XGET ${ES_URL}/_count?pretty

#info "Sample index data query"
#curl -k -XGET ${ES_URL}/_search?pretty=true -d ' {
#    "query": {
#        "query_string": {
#            "query": "EMAIL"
#        }
#    }
#}
#'
