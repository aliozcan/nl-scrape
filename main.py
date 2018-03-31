import ipdb
import yaml
import json
import requests
import pandas as pd
import psycopg2
import datetime as dt
import logging
import time
from itertools import cycle

logger = logging.getLogger('istapp')
hdlr = logging.FileHandler('istapp.log')
formatter = logging.Formatter('%(asctime)s;%(levelname)s;%(message)s;')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)

url = 'https://api.foursquare.com/v2/venues/search'

cf = yaml.load(open('.env', 'r'))


CLIENT_IDS, CLIENT_SECRETS = cycle(cf['client_ids']), cycle(cf['client_secrets'])

params = dict(
  client_id=next(CLIENT_IDS),
  client_secret=next(CLIENT_SECRETS),
  intent='browse',
  sw='',
  ne='',
  v=dt.datetime.now().date().strftime('%Y%m%d'),
  limit=50
)

#conn = psycopg2.connect(f"dbname={cf['db']['name'] user={cf['db']['user']} host={cf['db']['host']} password={cf['db']['host']}')

grids = open('rotterdam-grids.csv', 'r')
result = {}
total_req = 1
key_pos = 0

res = {int(line.split(';')[0]): line.split(';')[1].strip() for line in open('result.csv', 'r')}

# get grids
# flip for google
# POLYGON((4.00512754910515 51.841812133789,4.00512754910515 51.8427108942292,4.00657851038549 51.8427108942292,4.00657851038549 51.841812133789,4.00512754910515 51.841812133789))
# SW, NW, NE, SE, SW

grid, count = grids.readline(), 1

#ipdb.set_trace()
while grid: 
    if count in res.keys() :
        count += 1
        grid = grids.readline()
        continue
    
    bbox = grid.replace('POLYGON((', '').replace(')', '').split(',')
    sw, ne = f'{bbox[0].split()[1]},{bbox[0].split()[0]}', f'{bbox[2].split()[1]},{bbox[2].split()[0]}'
    params.update({'sw': sw, 'ne':ne})
    try:
        resp = requests.get(url=url, params=params)
        if resp.status_code == 200:
            data = json.loads(resp.text)
            data = pd.DataFrame(data['response']['venues'], columns=['id'])
            result[count] = ','.join(data['id'])
            logger.info(f'{count}, {len(data["id"])}')
            grid = grids.readline()
            count += 1
        elif resp.status_code in [429, 403]:
            # daily quota exceed. wait until midnight.
            logger.info(f'switching keys to {key_pos + 1}')
            params.update({'client_id': next(CLIENT_IDS), 'client_secret': next(CLIENT_SECRETS)})
            logger.warning(f'grid:{count}, status:{resp.status_code}')
            if key_pos > len(cf['client_ids']):
                # if we tried with all keys then sleep
                with open('result.csv', 'a+') as f:
                    for k,v in result.items():
                        f.write(f'{k};{v}\n')
                if resp.status_code == 403:
                    logger.info('all keys are done. exceed hourly sleeping 1 hr')
                    time.sleep(60*60)
                elif resp.status_code == 429:
                    logger.info('all keys are done. hit the daily limit. sleeping until midnight')
                    tomorrow = dt.datetime.now() + dt.timedelta(1)
                    midnight = dt.datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=0, minute=0, second=0)
                    wt = (midnight - dt.datetime.now()).seconds
                    time.sleep(wt + 60)
                key_pos = 0
            else:
                key_pos += 1
    except requests.HTTPError as e:
        logger.warning(f'grid:{count}, {e}')

with open('result.csv', 'a+') as f:
    for k,v in result.items():
        f.write(f'{k};{v}\n')


