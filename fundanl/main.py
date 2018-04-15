import datetime as dt
import json
import logging
import random
import re
import schedule
import time
import psycopg2
from itertools import chain
from collections import OrderedDict
from requests_html import HTMLSession

_connection = f'host=postgres user=postgres password=postgres'
conn = psycopg2.connect(_connection)

def requests_html():
    #daily job
    headers = {
        'Host': 'www.funda.nl',
        'Connection': 'keep-alive',
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36",
        'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9'
    }
    session = HTMLSession()
    
    urls = ['https://www.funda.nl/koop/gemeente-rotterdam/', 
            'https://www.funda.nl/koop/berkel-en-rodenrijs/',
            'https://www.funda.nl/huur/gemeente-rotterdam/']
    
    max_page_regex = r'p([0-9]+)/$'
    huis_link_regex = r'.+(huis|appartement)-\d+.+/'
    link_regex = r'(\w+)/([a-zA-Z-_]+)/(huis|appartement)-\d+.+' # koop/rotterdam/huis-40536936-petrus-trousselotstr

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
 
    for region_url in urls:
        r = session.get(region_url, headers=headers, timeout=5)
        max_page = [re.search(max_page_regex, link) for link in r.html.links]
        max_page = max([int(page.group(1)) for page in max_page if page])
        logger.info(f'Max page: {max_page}')
        result = OrderedDict()
        try:
            for page in range(1, max_page + 1):
                url = f'{region_url}p{page}/'           
                r = session.get(url, headers=headers)
                houses = [re.search(huis_link_regex, link) for link in r.html.links]
                houses = [f'https://www.funda.nl{link.group(0)}' for link in houses if link]
                house_count = len(houses)
                logger.info(f'starting page {page}, url: {url}, # houses: {house_count}')
                houses = [house for house in houses if re.search(huis_link_regex, house).group(0).replace('/', '') not in existing_houses]
                logger.info(f'{house_count - len(houses)} links already scraped.')
                
                sleep_t = random.randint(2, 4)
                time.sleep(sleep_t)

                for count, house_url in enumerate(houses):
                    house_url_postfix = re.search(link_regex, house_url).group(0)
                    existing_houses.add(house_url_postfix)
                    logger.info(f'page {page}, house {count + 1}, url: {house_url} parsing')

                    r = session.get(house_url, headers=headers)
                    house = OrderedDict()
                    house['Date'] = str(dt.datetime.now())
                    house['Address'] = r.html.find('h1', first=True).text
                    house['Price'] = r.html.find('strong', first=True).text

                    #Bouwjaar, Woonoppervlakte, Aantal kamers, Gelegen op
                    for item in r.html.find(selector='li.kenmerken-highlighted__list-item'):
                        text = item.text
                        if 'Aantal' in text: # Aantal kamers
                            k, v = ' '.join(text.split(' ')[:2]), text.split(' ')[2]
                        else:
                            k, v = text.split(' ', 1)
                        house[k] = v
                    house['Description'] = r.html.find(selector='div.object-description-body', first=True).text.strip()

                    for item in r.html.find(selector='dl.object-kenmerken-list'):
                        sub_item_list = []
                        for sub_item in item.text.split('\n'):
                            if sub_item not in 'Gebruiksoppervlakten' or sub_item.startswith('Gebruiksoppervlakten') == False:
                                    sub_item_list.append(sub_item)
                        house.update({k.strip():v.strip() for k,v in zip(sub_item_list[::2], sub_item_list[1::2])})
                    result[house_url_postfix] = house
                    sleep_t = random.randint(10, 15)
                    time.sleep(sleep_t)
        except (Exception, KeyboardInterrupt) as e:
            logger.info(f'page: {count}, house: {count}, url:{house_url}: {e}')
        finally:
            write_results_to_db(result)


def write_results_to_db(result):
    try:
        cur = conn.cursor()
        for k, v in result.items():
            cur.execute("insert into fundanl(url, body) values (%s, %s)", (k, json.dumps(v)))
            conn.commit()
            logger.info(f'{len(result)} items are written.')
    except Exception as e:
        logger.info(f'{e}')
        conn.rollback()
    finally:
        cur.close()



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
    schedule.every().day.at('12:00').do(requests_html)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    logger = logging.getLogger('fundanl')
    hdlr = logging.FileHandler('output/fundanl.log')
    formatter = logging.Formatter('%(asctime)s;%(levelname)s;%(message)s;')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)
    main()
