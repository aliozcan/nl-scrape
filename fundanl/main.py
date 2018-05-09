import datetime as dt
import json
import logging
import random
import re
import schedule
import time
import sys
import os
import psycopg2
from itertools import chain
from collections import OrderedDict
from requests_html import HTMLSession
from scrape import scrape
from proxy import get_headers
from db import conn

def requests_html():
    #daily job
    session = HTMLSession()
    
    urls = ['https://www.funda.nl/koop/gemeente-rotterdam/', 
            'https://www.funda.nl/koop/berkel-en-rodenrijs/',
            'https://www.funda.nl/huur/gemeente-rotterdam/',
            'https://www.funda.nl/huur/gemeente-amsterdam/',
            'https://www.funda.nl/koop/gemeente-amsterdam/']
    
    max_page_regex = r'p([0-9]+)/$'
    huis_link_regex = r'.+(huis|appartement)-\d+.+/'
    link_regex = r'(\w+)/([a-zA-Z-_]+)/(huis|appartement)-\d+.+' # koop/rotterdam/huis-40536936-petrus-trousselotstr

    existing_houses = get_scraped_results()

 
    for region_url in urls:
        for _ in range(10):
            try:
                r = session.get(region_url, headers=get_headers(), timeout=5)
            except Exception as e:
                time.sleep(random.randint(60, 150))
                pass
            else:
                break

        max_page = [re.search(max_page_regex, link) for link in r.html.links]
        max_page = max([int(page.group(1)) for page in max_page if page])
        logger.info(f'Max page: {max_page}')
        try:
            for page in range(1, max_page + 1):
                url = f'{region_url}p{page}/'           
                r = session.get(url, headers=get_headers(), timeout=5)
                houses = [re.search(huis_link_regex, link) for link in r.html.links]
                houses = [f'https://www.funda.nl{link.group(0)}' for link in houses if link]
                house_count = len(houses)
                logger.info(f'starting page {page}, url: {url}, # houses: {house_count}')
                houses = [house for house in houses if re.search(huis_link_regex, house).group(0).replace('/', '') not in existing_houses]
                logger.info(f'{house_count - len(houses)} links already scraped.')
                for count, house_url in enumerate(houses):
                    house_url_postfix = re.search(link_regex, house_url).group(0)
                    existing_houses.add(house_url_postfix)
                    scrape.delay(house_url)
                    logger.info(f'Task submitted: page: {page}, house: {count + 1}, url: {house_url}')
        except (Exception, KeyboardInterrupt) as e:
            logger.info(f'page: {count}, house: {count}, url:{house_url}: {e}')


def get_scraped_results() -> set:
    try:
        cur = conn.cursor()
        cur.execute('select distinct url from fundanl')
        existing_houses = cur.fetchall()
        existing_houses = set(chain(*existing_houses))
    except Exception as e:
        logger.info(f'{e}')
        existing_houses = set()
    finally:
        cur.close()
        return existing_houses


def init_db():
    try:
        cur = conn.cursor()
        cur.execute('create table if not exists fundanl(url text, body json);')
        conn.commit()
    except:
        conn.rollback()
    finally:
        cur.close()
    

def main():
    init_db()
    requests_html()
    schedule.every().day.at('07:00').do(requests_html)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    logger = logging.getLogger('fundanl')
    hdlr = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s;%(levelname)s;%(message)s;')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)
    main()
