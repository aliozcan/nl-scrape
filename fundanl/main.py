import logging
import random
import re
import schedule
import time
import sys
from requests_html import HTMLSession
from scrape import scrape
from proxy import get_headers
from db import get_scraped_results, init_db

def requests_html():
    #daily job
    session = HTMLSession()
    
    urls = open('urls.txt', 'r').read().split('\n')
    random.shuffle(urls)
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
                houses = [house for house in houses if re.search(link_regex, house).group(0) not in existing_houses]
                logger.info(f'{house_count - len(houses)} links already scraped.')
                random.shuffle(houses)
                for count, house_url in enumerate(houses):
                    house_url_postfix = re.search(link_regex, house_url).group(0)
                    existing_houses.add(house_url_postfix)
                    scrape.delay(house_url)
                    logger.info(f'Task submitted: page: {page}/{max_page}, house: {count + 1}/{house_count - len(houses)}, url: {house_url}')
                    time.sleep(random.randint(20, 30))
                time.sleep(random.randint(60, 150))
        except (Exception, KeyboardInterrupt) as e:
            logger.info(e)


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
