#!/usr/bin/env python

import argparse
import logging

import ConfigParser
import cx_Oracle

import smtplib

logging.basicConfig(format='[%(levelname)s]: %(message)s')
logger = logging.getLogger('QB')

parser = argparse.ArgumentParser()
parser.add_argument("--config", help="oracle connection config", required=True)
parser.add_argument("--days", help="select froms for N last days", type=int, default=60)
parser.add_argument("--email", help="from email address for test emails", default="check-froms-delivery@qnoon.com")
parser.add_argument("--username", help="username part for test emails", default="checkactivefroms183")
parser.add_argument("--debug", help="print debug information", action="store_true")
parser.add_argument("--dry-run", help="avoid sending data to telegraf", action="store_true")
args = parser.parse_args()

if args.debug:
  logger.setLevel(logging.DEBUG)

logger.debug( "Arguments:" )
logger.debug( args )

config = args.config

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
else:
  smtp_server = smtplib.SMTP('localhost')

  for domain in froms:
    to_email = "{}@{}".format(args.username, domain)
    message = "To: {}".format(to_email)
    smtp_server.sendmail(args.email, to_email, message)

