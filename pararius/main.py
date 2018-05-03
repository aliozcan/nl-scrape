import datetime as dt
import json
import logging
import random
import re
import schedule
import time
import sys
import psycopg2
from itertools import chain
from collections import OrderedDict
from requests_html import HTMLSession, HTML

_connection = f'host=postgres user=postgres password=postgres'
conn = psycopg2.connect(_connection)

def requests_html():
    #daily job
    headers = {
        'Host': 'www.pararius.com',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36",
        'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9'
    }
    session = HTMLSession()
    r = session.get('https://www.pararius.com/', headers=headers, timeout=5)
    if r.status_code == 200:
        k,v = session.cookies.get_dict().popitem()
        headers.update({'Cookie': f'{k}={v}'})
    
    sleep_t = random.randint(6, 15)
    time.sleep(sleep_t)
    
    urls = ['https://www.pararius.com/apartments/rotterdam/',
            'https://www.pararius.com/apartments/den-haag/',
            'https://www.pararius.com/apartments/amsterdam/']
    
    max_page_regex = r'page-([0-9]+)$'
    link_regex = r'([a-zA-Z-_]+)/([a-zA-Z-_]+)/([A-Z0-9]+/(\w+))' # apartment-for-rent/rotterdam/PR0001447944/hoevestraat

    try:
        cur = conn.cursor()
        cur.execute('select distinct url from pararius')
        existing_houses = cur.fetchall()
        existing_houses = set(chain(*existing_houses))
    except Exception as e:
        logger.info(f'{e}')
        existing_houses = set()
    finally:
        cur.close()
 
    for region_url in urls:
        r = session.get(region_url, headers=headers, timeout=5)
        if r.status_code != 200:
            raise ValueError(f'Status code: {r.status_code}')

        max_page = [re.search(max_page_regex, link) for link in r.html.links]
        max_page = max([int(page.group(1)) for page in max_page if page])
        logger.info(f'Max page: {max_page}')
        result = OrderedDict()
        try:
            for page in range(1, max_page + 1):
                if page > 1:
                    url = f'{region_url}page-{page}/'
                    r = session.get(url, headers=headers)
                else:
                    url = region_url

                houses = [re.search(link_regex, link) for link in r.html.links]
                houses = [f'https://www.pararius.com/{link.group(0)}/' for link in houses if link]
                house_count = len(houses)
                logger.info(f'starting page {page}, url: {url}, # houses: {house_count}')
                houses = [house for house in houses if re.search(link_regex, house).group(0) not in existing_houses]
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

                    details = HTML(html=r.html.find('div#details', first=True).html)

                    house['Inclusive'] = r.html.find('span.inclusive', first=True).text
                    for k, v in zip(details.find('dt'), details.find('dd')):
                        house[k.text] = v.text

                    features = HTML(html=details.find('ul.features', first=True).html).find('li')
                    house_features = []
                    for feature in features:
                        if ':' in feature.text:
                            k, v = feature.text.split(':')
                            house[k] = v
                        else:
                            house_features.append(feature.text)
                    else:
                        house['Features'] = house_features

                    span_elements = r.html.find('span')
                    house['Description'] = r.html.find('p.text', first=True).text


                    for span in span_elements:
                        if 'itemprop' in span.attrs:
                            house['Type'], house['Street'], house['City'] = span.text.split('\n')
                    
                    house['Surface'] = r.html.find('li.surface', first=True).text
                    house['Bedrooms'] = r.html.find('li.bedrooms', first=True).text
                    house['Furniture'] = r.html.find('li.furniture', first=True).text

                    result[house_url_postfix] = house
                    sleep_t = random.randint(10, 15)
                    time.sleep(sleep_t)
        except (Exception, KeyboardInterrupt) as e:
            logger.info(f'page: {count}, house: {count}, url:{house_url}: {e}')
        finally:
            write_results_to_db(result)
            result = OrderedDict()
    return 

def write_results_to_db(result):
    try:
        cur = conn.cursor()
        for k, v in result.items():
            cur.execute("insert into pararius(url, body) values (%s, %s)", (k, json.dumps(v)))
            conn.commit()
    except Exception as e:
        logger.info(f'{e}')
        conn.rollback()
    else:
        logger.info(f'{len(result)} items are written.')
    finally:
        cur.close()



def init_db():
    try:
        cur = conn.cursor()
        cur.execute('create table if not exists pararius(url text, body json);')
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
    logger = logging.getLogger('pararius')
    hdlr = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s;%(levelname)s;%(message)s;')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)
    main()
