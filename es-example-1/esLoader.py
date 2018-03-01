#!/usr/bin/env python
import getopt
import sys
import os
import json
import boto3
import logging
import requests
from datetime import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from requests_aws4auth import AWS4Auth

USAGE_INFO = """
Usage: esLoader.py [switches] [argument]
  -b, --batchSize
  -s, --batchScale
  -l, --logLevel
  -f, --inputFilename
  -e, --esEndpoint
Example:
  esLoader.py -b 100 -s 2 -l DEBUG -f xyz.csv -e abcdef.us-east-1.es.amazonaws.com
"""

ES_INDEX_NAME = "bl_edr"
ES_INDEX_TYPE = "edr"


def connect_to_es(pm_esEndpoint):
    try:
        es_auth = AWS4Auth(
            os.environ['AWS_ACCESS_KEY'],
            os.environ['AWS_SECRET_KEY'],
            os.environ['AWS_DEFAULT_REGION'],
            'es'
        )
        es_handle = Elasticsearch(host=pm_esEndpoint,
                                  port=443,
                                  http_auth=es_auth,
                                  use_ssl=True,
                                  verify_certs=True,
                                  connection_class=RequestsHttpConnection)
        logger.info('Connected to ES. Endpoint: {}'.format(pm_esEndpoint))
        logger.info('ES Info: {}'.format(es_handle.info()))
        return es_handle
    except Exception as e:
        logger.error(e)
        raise e
    return False


def create_index_es(pm_es):
    try:
        es_res = pm_es.indices.exists(ES_INDEX_NAME)
        if es_res is False:
            es_indexDoc = {
                "mappings": {
                    "edr": {
                        "properties": {
                            "PROCESS_ID": {
                                "type": "string"
                            },
                            "COMPONENT_ID": {
                                "type": "string"
                            },
                            "TRANSACTION_ID": {
                                "type": "string"
                            },
                            "SESSION_ID": {
                                "type": "string"
                            },
                            "EVENT_TIMESTAMP": {
                                "type": "string"
                            },
                            "EVENT_TYPE": {
                                "type": "string"
                            },
                            "DIVISION_ID": {
                                "type": "string"
                            },
                            "GROUP_ID": {
                                "type": "string"
                            },
                            "SUBSCRIBER_ID": {
                                "type": "string"
                            },
                            "SUBSCRIBER_TYPE": {
                                "type": "string"
                            },
                            "DEVICE_ID": {
                                "type": "string"
                            },
                            "PLAN_ID": {
                                "type": "string"
                            },
                            "NOTIFICATION_TYPE": {
                                "type": "string"
                            },
                            "NOTIFICATION_ADDRESS": {
                                "type": "string"
                            },
                            "USAGE": {
                                "type": "long"
                            },
                            "CHARGE_AMOUNT": {
                                "type": "double"
                            },
                            "ZZ_FILENAME": {
                                "type": "string"
                            },
                            "ZZ_BATCH_ID": {
                                "type": "long"
                            },
                            "ZZ_PROCESSED_DATETIME": {
                                "type": "date",
                                "format": "date_hour_minute_second_millis"
                            },
                            "ZZ_PARTITION_KEY": {
                                "type": "string"
                            },
                            "ZZ_EVENT_TS_ISO": {
                                "type": "date",
                                "format": "date_hour_minute_second_millis"
                            },
                            "ZZ_EVENT_TS_EPOC": {
                                "type": "date",
                                "format": "epoch_second"
                            }
                        }
                    }
                },
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            }
            es_res = pm_es.indices.create(
                index=ES_INDEX_NAME, body=es_indexDoc)
            logger.info(es_res)
            return es_res
    except Exception as e:
        logger.error(e)
        raise e
    return False


def send_to_es(pm_es, pm_data, pm_batchSize):
    # logger.debug('__pm_data: {}'.format(json.dumps(pm_data)))
    # todo: streamline batch processing, no need to do it twice
    try:
        es_actions = []
        for event in pm_data[0]['ZZ_Data']:
            es_actions.append({
                "_op_type": "index",
                "_index": ES_INDEX_NAME,
                "_type": ES_INDEX_TYPE,
                "_id": event['TRANSACTION_ID'],
                "_source": event

            })
        num_success, error_list = helpers.bulk(pm_es, es_actions,
                                               index=ES_INDEX_NAME, doc_type=ES_INDEX_TYPE, chunk_size=pm_batchSize)
        logger.info('ES bulk insert success count: {}'.format(num_success))
        logger.error('ES bulk insert errors: {}'.format(error_list))
        return True
    except Exception as e:
        logger.error(e)
        raise e
    return False


def ingest_file(pm_esHandle, pm_fileName, pm_batchSize, pm_batchScale):
    try:
        delimiter = ','
        with open(pm_fileName) as infile:
            line = infile.readline()
            header = [header.strip()
                      for header in line.split(delimiter)[0:]]
            header = header[: -1]
            header.append("ZZ_FILENAME")
            header.append("ZZ_BATCH_ID")
            header.append("ZZ_PROCESSED_DATETIME")
            header.append("ZZ_PARTITION_KEY")
            header.append("ZZ_EVENT_TS_ISO")
            header.append("ZZ_EVENT_TS_EPOC")
            record_count = 0
            idx = 0
            mod = 0
            line_len = 1
            while line_len != 0:
                metadata = {
                    "ZZ_FileName": "%s" % pm_fileName,
                    "ZZ_BatchSize": "%s" % pm_batchSize,
                    "ZZ_BatchScale": "%s" % pm_batchScale,
                    "ZZ_BatchId": "%s" % idx,
                    "ZZ_Data": []
                }
                payload = []
                for i in range(pm_batchSize):
                    line = infile.readline()
                    line_len = len(line)
                    if line_len == 0:
                        break
                    data_list = [data.strip()
                                 for data in line.split(delimiter)[0:]]
                    data_list = data_list[:-1]
                    now = datetime.utcnow()
                    event_ts = datetime.strptime(
                        data_list[4], '%Y/%m/%d %H:%M:%S.%f')

                    # Composite key items
                    division_id = data_list[6]
                    subscriber_id = data_list[8]
                    subscriber_type = data_list[9]
                    device_id = data_list[10]
                    partition_key = '{}|{}|{}|{}'.format(
                        device_id,
                        subscriber_id,
                        subscriber_type,
                        division_id
                    )

                    event_ts_epoc = (
                        event_ts - datetime(1970, 1, 1)).total_seconds()

                    data_list.append(pm_fileName)
                    data_list.append(idx)

                    # Truncating precision of micro seconds due to ES constraints
                    data_list.append(str(now.isoformat())[:-3])

                    data_list.append(partition_key)

                    # Truncating precision of micro seconds due to ES constraints
                    data_list.append(str(event_ts.isoformat())[:-3])

                    # Rounding decimals to whole due to ES constraints
                    data_list.append(int(round(event_ts_epoc)))

                    body = dict(zip(header, data_list[0:]))
                    metadata['ZZ_Data'].append(body)
                    record_count += 1
                payload.append(metadata)
                send_to_es(pm_esHandle, payload, pm_batchSize)
                mod = idx % pm_batchScale
                idx += 1
    except Exception as e:
        logger.error(e)
        raise e


def main(argv):
    if len(argv) == 0:
        print(USAGE_INFO)
        return 1
    try:
        opts, args = getopt.getopt(argv, "hb:s:l:f:e:",
                                   ["help",
                                    "batchSize=",
                                    "batchScale=",
                                    "logLevel=",
                                    "inputFilename=",
                                    "esEndpoint="])
    except Exception, err:
        sys.stderr.write("Exception:: %s\n" % str(err))
        print(USAGE_INFO)
        return 1

    global logger

    sp_batchSize = None
    sp_batchScale = None
    sp_logLevel = None
    sp_inputFilename = None
    sp_esEndpoint = None

    for opt, arg in opts:
        if opt in ('-h', "--help"):
            print(USAGE_INFO)
            return 1
        elif opt in ('-b', "--batchSize"):
            sp_batchSize = int(arg)
        elif opt in ('-s', "--batchScale"):
            sp_batchScale = int(arg)
        elif opt in ('-l', "--logLevel"):
            sp_logLevel = arg
        elif opt in ('-f', "--inputFilename"):
            sp_inputFilename = arg
        elif opt in ('-e', "--esEndpoint"):
            sp_esEndpoint = arg

    if (sp_batchSize is not None) and \
        (sp_batchScale is not None) and \
        (sp_logLevel is not None) and \
        (sp_inputFilename is not None) and \
            (sp_esEndpoint is not None):

        log_filename = str(datetime.now().strftime(
            'esLoader_%Y%m%d_%H%M%S.log'))
        logging.basicConfig(filename=log_filename)
        logger = logging.getLogger()

        if sp_logLevel == 'WARN':
            logger.setLevel(logging.WARN)
        elif sp_logLevel == 'INFO':
            logger.setLevel(logging.INFO)
        elif sp_logLevel == 'DEBUG':
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.DEBUG)

        es_handle = connect_to_es(sp_esEndpoint)
        if es_handle is not False or None:
            es_res = create_index_es(es_handle)
            if es_res is not False or None:
                ingest_file(es_handle,
                            sp_inputFilename,
                            sp_batchSize,
                            sp_batchScale)
        return 0
    else:
        print(USAGE_INFO)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
