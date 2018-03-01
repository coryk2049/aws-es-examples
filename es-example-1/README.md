## es-example-1

Elasticsearch batch data loader using Python ES client and bulk provisioning API.

### Prerequisites

1) Create ES service linked role prior to deploying CF template

`aws iam create-service-linked-role --aws-service-name es.amazonaws.com`

Note: Not possible to do with CF at this time according to AWS

2) Deploy the CF template:

https://github.com/coryk2049/aws-cf-examples/tree/master/cf-example-11/cf-example-11.json

3) Configure SSH tunnel for ES remote access from your laptop if required

https://github.com/coryk2049/aws-cf-examples/tree/master/cf-example-11/README.md

4) Update AWS access credentials in `dot.profile`

5) Update ES endpoint in `testRun.sh`

6) Upload `esLoader.py`, `dot.profile`, `testRun.sh` and `pcrf*edr*.csv` onto EC2 instance

Note: No need to perform steps (4-6) if SSH tunneling, else setup virtual environment via `setupEsLoaderEnv.sh`

### Execute ES Loader

Execute `testRun.sh` script to perform ETL of the `pcrf*edr*.csv` input file into ES.

Review runtime `esLoader*.log` for low level steps performed.

### Index Query

Once ETL has successfully completed, inspect index `"bl_edr"` with `@timestamp "ZZ_EVENT_TS_ISO"` for results.

### References
- http://www.pythonexample.com/code/elasticsearch-bulk-index-json
- https://stackoverflow.com/questions/20288770/how-to-use-bulk-api-to-store-the-keywords-in-es-by-using-python
- https://stackoverflow.com/questions/44747836/add-timestamp-field-in-elasticsearch-with-python
- https://github.com/coryk2049/data-gen/tree/master/pcrf
- https://github.com/coryk2049/aws-cf-examples/tree/master/cf-example-11
