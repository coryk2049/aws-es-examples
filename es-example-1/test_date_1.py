#!/usr/bin/env python
import getopt
import sys
import os
import json
import requests
from datetime import datetime
#raw_event_ts = "2017/01/01 00:00:01.696872"
raw_event_ts = "2017/01/01 00:00:01.123456"
event_ts = datetime.strptime(raw_event_ts, '%Y/%m/%d %H:%M:%S.%f')
event_ts_epoc = (event_ts - datetime(1970, 1, 1)).total_seconds()
event_ts_epoc2 = int(round(event_ts_epoc))
print("event_ts: {}".format(str(event_ts)[:-3]))
print("event_ts_iso: {}".format(str(event_ts.isoformat())[:-3]))
print("event_ts_epoc: {}".format(event_ts_epoc))
print("event_ts_epoc2: {}".format(event_ts_epoc2))
