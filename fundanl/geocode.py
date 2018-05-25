import schedule
import time
import logging
import sys
from geocoder.tasks import geocode
from db import get_missing_url_coordinates


def iter_urls():
    result = get_missing_url_coordinates()
    for url, address in result:
        geocode.apply_async(args=[url, address])


def main():
    iter_urls()
    schedule.every().day.at('07:30').do(iter_urls)
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
