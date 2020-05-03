#!/usr/bin/env python

from telegraf.client import TelegrafClient

import argparse
import logging

import os

logging.basicConfig(format='[%(levelname)s]: %(message)s')
logger = logging.getLogger('QB')

parser = argparse.ArgumentParser()
parser.add_argument("--metric", help="metric name", required=True)
parser.add_argument("--queue", help="queue name", required=True)
parser.add_argument("--path", help="queue folder path", required=True)
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
queue_name = args.queue
queue_path = args.path

tg = TelegrafClient(host=args.host, port=args.port)

path, dirs, files = next(os.walk(queue_path))

queue_len = len(files)

logger.debug( "Queue '{}' on {} has length: {}".format(queue_name, queue_path, queue_len) )

if args.dry_run:
  logger.debug( "Dry-run mode activated, skipping sending data to telegraf" )
else:
  tg.metric( metric, queue_len, tags={ 'queue': queue_name, 'path': queue_path })

