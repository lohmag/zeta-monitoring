#!/usr/bin/env python

from telegraf.client import TelegrafClient

import argparse
import logging

import sys

import requests
import time
import math

logging.basicConfig(format='[%(levelname)s]: %(message)s')
logger = logging.getLogger('QB')

parser = argparse.ArgumentParser()
parser.add_argument("--metric", help="metric name", required=True)
parser.add_argument("--etalon", help="etalon name", required=True)
parser.add_argument("--url", help="etalon url", required=True)
parser.add_argument("--header", help="etalon header", default='X-Now')
parser.add_argument("--host", help="telegraf host", default='127.0.0.1')
parser.add_argument("--port", help="telegraf port", type=int, default=8094)
parser.add_argument("--debug", help="print debug information", action="store_true")
parser.add_argument("--dry-run", help="avoid sending data to telegraf", action="store_true")
args = parser.parse_args()

if args.debug:
  logger.setLevel(logging.DEBUG)

logger.debug( "Arguments:" )
logger.debug( args )

metric = args.metric
etalon_name = args.etalon
etalon_url = args.url
etalon_header = args.header

tg = TelegrafClient(host=args.host, port=args.port)

r = requests.get( etalon_url )
logger.debug("Etalon response headers:")
logger.debug( r.headers )

if etalon_header not in r.headers:
  logger.error( "Header {} not found in etalon response".format(etalon_header) )
  sys.exit(-1)

etalon_time = r.headers[etalon_header]
logger.debug( "Etalon timestamp: {}".format(etalon_time) )

local_time = time.time()
logger.debug( "Local timestamp: {}".format(local_time) )

time_diff = int( abs( float(etalon_time) - local_time ) )
logger.debug( "Time diff: {}".format(time_diff) )


if args.dry_run:
  logger.debug( "Dry-run mode activated, skipping sending data to telegraf" )
else:
  tg.metric( metric, time_diff, tags={ 'etalon': etalon_name, 'url': etalon_url })

