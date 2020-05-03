#!/usr/bin/env python

from telegraf.client import TelegrafClient

import argparse
import logging

import ConfigParser
import cx_Oracle

from datetime import datetime, timedelta

logging.basicConfig(format='[%(levelname)s]: %(message)s')
logger = logging.getLogger('QB')

parser = argparse.ArgumentParser()
parser.add_argument("--metric", help="metric name", required=True)
parser.add_argument("--config", help="oracle connection config", required=True)
parser.add_argument("--max", help="check N next days", type=int, default=60)
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
config = args.config

tg = TelegrafClient(host=args.host, port=args.port)

# partitions schema
parts = [
    { 'table': 'TRACKSYS_SENTLOG', 'prefix': 'SENTLOG_', 'left': args.max },
    { 'table': 'TRACKSYS_RAWLOG',  'prefix': 'TRLOG_',   'left': args.max },
]

# read config
config = ConfigParser.RawConfigParser()
config.read( args.config )

ora_user = config.get('tracksys', 'login')
ora_pass = config.get('tracksys', 'password')
ora_dsn = config.get('tracksys', 'dsn')

logger.debug( "Oracle connection string: {}/@{}".format(ora_user, ora_pass, ora_dsn) )

# Oracle part
con = cx_Oracle.connect( ora_user, ora_pass, ora_dsn )
cur = con.cursor()
cur.prepare('SELECT PARTITION_NAME FROM USER_TAB_PARTITIONS WHERE TABLE_NAME = :tbname')

partitions = []

for schema in parts:
  cur.execute(None, { 'tbname': schema['table'] })
  partitions += map( lambda x: x[0], cur.fetchall() )

cur.close()
con.close()

# Monitoring part
today = datetime.now()

for day in reversed(  xrange(args.max) ):
  delta = timedelta( days=day )
  check_date = (today + delta).strftime('%Y%m%d')

  for schema in parts:
    if schema['prefix'] + check_date not in partitions:
      schema['left'] = day

for schema in parts:
  logger.debug( "{} partitions left: {}".format(schema['table'], schema['left']) )


if args.dry_run:
  logger.debug( "Dry-run mode activated, skipping sending data to telegraf" )
else:
  for schema in parts:
    tg.metric( metric, schema['left'], tags={ 'table': schema['table'] } )

