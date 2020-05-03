#!/usr/bin/env python

from telegraf.client import TelegrafClient

import argparse
import logging

import os
import sys
import time

import ConfigParser
import cx_Oracle

logging.basicConfig(format='[%(levelname)s]: %(message)s')
logger = logging.getLogger('QB')

parser = argparse.ArgumentParser()
parser.add_argument("--metric", help="metric name", required=True)
parser.add_argument("--config", help="oracle connection config", required=True)
parser.add_argument("--days", help="select froms for N last days", type=int, default=60)
parser.add_argument("--spool", help="spool directory path", required=True)
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
spool = args.spool

if not os.path.isdir(spool):
  logger.critical( "Spool {} is not directory".format(spool) )
  sys.exit(2)

tg = TelegrafClient(host=args.host, port=args.port)

# read config
config = ConfigParser.RawConfigParser()
config.read( args.config )

ora_user = config.get('qman', 'login')
ora_pass = config.get('qman', 'password')
ora_dsn = config.get('qman', 'dsn')

logger.debug( "Oracle connection string: {}/{}@{}".format(ora_user, ora_pass, ora_dsn) )

# Oracle part
con = cx_Oracle.connect( ora_user, ora_pass, ora_dsn )
cur = con.cursor()
cur.prepare('SELECT DISTINCT from_domain FROM BORMAN_IFU_DELIVERED_STATS WHERE mailing_date > systimestamp - :days ORDER BY from_domain')
cur.execute(None, { 'days': args.days })

froms = map( lambda x: x[0], cur.fetchall() )

cur.close()
con.close()

logger.debug( "Active froms found: {}".format( len(froms)) ) 
logger.debug(froms)

if args.dry_run:
  logger.debug( "Dry-run mode activated, skipping sending emails" )

for domain in froms:
  domain_file = spool + '/' + domain

  mod_time = 10000000

  if os.path.isfile(domain_file):
    mod_time = int(time.time() - os.path.getmtime(domain_file))
   
  logger.debug("Last {} delivery: {}".format(domain, mod_time) )
  tg.metric( metric, mod_time, tags={ 'from': domain } )

