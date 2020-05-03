#!/usr/bin/env python

import argparse
import logging

import os
import sys

logging.basicConfig(format='[%(levelname)s]: %(message)s')
logger = logging.getLogger('QB')

parser = argparse.ArgumentParser()
parser.add_argument("--spool", help="spool directory path", required=True)
parser.add_argument("--debug", help="print debug information", action="store_true")
parser.add_argument("--dry-run", help="avoid sending data to telegraf", action="store_true")
args = parser.parse_args()

if args.debug:
  logger.setLevel(logging.DEBUG)

logger.debug( "Arguments:" )
logger.debug( args )

spool = args.spool

if not os.path.isdir(spool):
  logger.critical( "Spool {} is not directory".format(spool) )
  sys.exit(2)

# touch
def touch(fname, times=None):
  with open(fname, 'a'):
    os.utime(fname, times)

domain = None

for line in sys.stdin:
  line.rstrip()

  entry = line.split()

  if len(entry) < 2:
    continue
  header = entry[0]
  value = entry[1]

  if header == 'To:':
    username, domain = value.split('@')
    break

if domain:
  logger.debug( "Domain found: {}".format(domain) )
else:
  logger.warning("Domain not found")
  sys.exit(0)


if args.dry_run:
  logger.debug( "Dry-run mode activated, skipping touching" )
else:
  touch( spool + '/' + domain)

