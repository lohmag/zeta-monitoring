#!/usr/bin/python3.6
import resource
resource.setrlimit(resource.RLIMIT_NOFILE, (500000, 500000))

from telegraf.client import TelegrafClient
import re
import argparse
import logging
import asyncio
import aiohttp
from aiohttp import ClientSession
import requests

logging.basicConfig(format='[%(levelname)s]: %(message)s')
logger = logging.getLogger('QB')

parser = argparse.ArgumentParser()
parser.add_argument("--metric", help="metric name", required=True)
parser.add_argument("--location", help="check location", required=True)
parser.add_argument("--header", help="etalon header", default='X-Check')
parser.add_argument("--host", help="telegraf host", default='127.0.0.1')
parser.add_argument("--port", help="telegraf port", type=int, default=8094)
parser.add_argument("--debug", help="print debug information", action="store_true")
parser.add_argument("--dry-run", help="avoid sending data to telegraf", action="store_true")
args = parser.parse_args()

if args.debug:
    logger.setLevel(logging.DEBUG)

logger.debug("Arguments:")
logger.debug(args)

metric = args.metric
check_location = args.location
check_header = args.header

tg = TelegrafClient(host=args.host, port=args.port)

# greylog_from = 'https://graylog.noc.dcapi.net:8443/api/search/universal/relative/terms?query=header_x-dc-from-domain%3A%2A&range=1209600&field=header_x-dc-from-domain&order=header_x-dc-from-domain%3Adesc&size=999999'
greylog_url = 'https://graylog.noc.dcapi.net:8443/api/search/universal/relative/terms?query=header_x-dc-url-domain%3A%2A&range=1209600&field=header_x-dc-url-domain&order=header_x-dc-url-domain%3Adesc&size=999999'
r_url = requests.get(greylog_url, auth=('XXX', 'token'))
urls_dirty = r_url.json()['terms']

logger.debug("Graylog response actual url links:")
logger.debug(urls_dirty)

urls = {}
for url in urls_dirty:
    if re.match(r'^[A-Za-z0-9]*\.?[a-zA-Z0-9][a-zA-Z0-9-]{1,61}[a-zA-Z0-9]\.[a-zA-Z]{2,}$', url) is not None:
        urls[url] = urls_dirty[url]

#urls = {'jnigp.fatalion.com': 586817, 'mbjl.pyncha.com': 580963, 'aksd.pcoewa,com': 29323}


async def fetch(session, url):
    for i in range(0, 6):
        try:
            async with session.get(url, timeout=20) as response:
                return response
        except Exception as e:
            if i == 5:
                return e
            continue


urls_check_list = {}


async def run(urls):
    tasks = []

    # Fetch all responses within one Client session,
    # keep connection alive for all requests.
    async with ClientSession() as session:
        tasks = [fetch(session, "http://" + url + "/_check") for url in urls]
        results = await asyncio.gather(*tasks)

        for url, result in zip(urls, results):
            if not isinstance(result, Exception):
                if 'X-Check' in result.headers:
                    # print(f'{url}: {result.headers["X-Check"]}')
                    urls_check_list[url] = 1
                else:
                    # print(f'{url}: {"HEADER_NF"}')
                    urls_check_list[url] = 3
            else:
                urls_check_list[url] = 3


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(urls))
    loop.run_until_complete(future)

logger.debug("Checking result:")
logger.debug(urls_check_list)

if args.dry_run:
    logger.debug("Dry-run mode activated, skipping sending data to telegraf")
else:
    pass
    for url, status in urls_check_list.items():
        tg.metric(metric, {'status': status}, tags={'url': url, 'location': check_location, 'header': check_header})
