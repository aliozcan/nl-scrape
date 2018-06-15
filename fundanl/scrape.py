import random
import re
import time
import schedule
import logging
import sys
from proxy import do_request
from scraper.tasks import scrape
from db import get_scraped_results


def crawl():
    # daily job
    urls = open('urls.txt', 'r').read().split('\n')
    random.shuffle(urls)
    landing_url = 'https://www.funda.nl'
    max_page_regex = r'p([0-9]+)/$'
    huis_link_regex = r'.+(huis|appartement)-\d+.+/'

    existing_houses = get_scraped_results()

    for region_url in urls:
        logger.info(f'region is starting: {region_url}')
        r = do_request(region_url, use_proxy=False, default_header=False)
        if r is None:
            logger.error(f'{region_url} gets no response back')
            continue
        max_page = [re.search(max_page_regex, link) for link in r.html.links]
        max_page = max([int(page.group(1)) for page in max_page if page])
        logger.info(f'{region_url} : max page: {max_page}')
        for page in range(1, max_page + 1):
            url = f'{region_url}p{page}/'
            r = do_request(url, use_proxy=False, default_header=False)
            houses = [re.search(huis_link_regex, l) for l in r.html.links]
            houses = [f'{landing_url}{l.group(0)}' for l in houses if l]
            house_count = len(houses)
            houses = [h for h in houses
                      if re.search(huis_link_regex, h).group(0)
                      not in existing_houses]
            scraped_house_count = house_count - len(houses)
            remaining_house_count = house_count - scraped_house_count
            logger.info(f'{page}/{max_page}, {scraped_house_count}/{house_count} link scraped')
            random.shuffle(houses)
            for count, house_url in enumerate(houses):
                existing_houses.add(house_url)
                scrape.apply_async(args=[house_url],
                                   countdown=random.randint(20, 40))
                logger.info(f'Submitted: page: {page}/{max_page},'
                            f' house: {count + 1}/{remaining_house_count},'
                            f' url: {house_url}')
            time.sleep(random.randint(40, 60))


def main():
    crawl()
    schedule.every().day.at('07:30').do(crawl)
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
