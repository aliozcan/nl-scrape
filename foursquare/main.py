import datetime as dt
import json
import logging
import time
from itertools import cycle

import pandas as pd
import psycopg2
import requests
import yaml


venue_search_url = 'https://api.foursquare.com/v2/venues/search'
venue_details_url = 'https://api.foursquare.com/v2/venues/{venue_id}'

cf = yaml.load(open('.env', 'r'))
CLIENT_IDS, CLIENT_SECRETS = cycle(cf['client_ids']), cycle(cf['client_secrets'])



def get_venues():
    '''
    get a set of venues then process.
    :return: file: venue_id: json{}
    '''
    venues = set()

    params = dict(
      client_id=next(CLIENT_IDS),
      client_secret=next(CLIENT_SECRETS),
      v=dt.datetime.now().date().strftime('%Y%m%d')
    )

    lines = open('venues_by_grid.csv', 'r') #34310 unique venues in R'dam
    for line in lines:
        grid_id, grid_venues = line.strip().split(';')
        if len(grid_venues) > 1 :
            venues.update(grid_venues.split(','))


    key_pos, count = 0, 1
    result = {line.split(';')[0]: line.split(';')[1].strip() for line in open('venue_result.csv', 'r')}
    venue = venues.pop()
    while venue:
        if venue in result:
            venue = venues.pop()
            continue

        try:
            resp = requests.get(url=venue_details_url.format(venue_id=venue), params=params)
            if resp.status_code == 200:
                data = json.loads(resp.text)
                #data = pd.DataFrame(data['response']['venues'], columns=['id'])
                result[venue] = str(data)
                logger.info(f'{count}, {venue}: 200')
                count += 1
                venue = venues.pop()
            elif resp.status_code in [429, 403]:
                # daily quota exceed. wait until midnight.
                params.update({'client_id': next(CLIENT_IDS), 'client_secret': next(CLIENT_SECRETS)})
                logger.info(f'venue: {count}, status:{resp.status_code}')
                logger.info(f'switching keys to {key_pos + 1}')
                if key_pos > len(cf['client_ids']):
                    # if we tried with all keys then sleep
                    with open('venue_result.csv', 'a+') as f:
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

    with open('venue_result.csv', 'a+') as f:
        for k, v in result.items():
            f.write(f'{k};{v}\n')


def get_venues_by_grid():
    '''
    :return: file: grid_id;venues_id with , seperated
    '''
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
    key_pos = 0

    res = {int(line.split(';')[0]): line.split(';')[1].strip() for line in open('result.csv', 'r')}

    # get grids
    # flip for google
    # POLYGON((4.00512754910515 51.841812133789,4.00512754910515 51.8427108942292,4.00657851038549 51.8427108942292,4.00657851038549 51.841812133789,4.00512754910515 51.841812133789))
    # SW, NW, NE, SE, SW

    grid, count = grids.readline(), 1

    while grid:
        if count in res.keys() :
            count += 1
            grid = grids.readline()
            continue

        bbox = grid.replace('POLYGON((', '').replace(')', '').split(',')
        sw, ne = f'{bbox[0].split()[1]},{bbox[0].split()[0]}', f'{bbox[2].split()[1]},{bbox[2].split()[0]}'
        params.update({'sw': sw, 'ne':ne})
        try:
            resp = requests.get(url=venue_search_url, params=params)
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


if __name__ == '__main__':
    logger = logging.getLogger('istapp')
    hdlr = logging.FileHandler('istapp.log')
    formatter = logging.Formatter('%(asctime)s;%(levelname)s;%(message)s;')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)

    #get_venues_by_grid()
    get_venues()