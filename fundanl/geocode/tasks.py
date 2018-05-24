import schedule
import sys
import time
import logging
from requests_html import HTMLResponse
from celery_app import app
from db import write_geocode_to_db, get_missing_url_coordinates
from celery.utils.log import get_task_logger
from proxy import get_session


logger = get_task_logger(__name__)


@app.task(bind=True, max_retries=3, default_retry_delay=10,
          task_time_limit=60, task_soft_time_limit=50)
def geocode(self, url: str, address: str) -> bool:
    session = get_session(use_proxy=False)
    url = 'https://maps.googleapis.com/maps/api/geocode/json'
    params = {'sensor': 'false',
              'address': f'{address}',
              'region': 'nl'}
    session.params = params
    try:
        r = session.get(url, timeout=5)
    except Exception as e:
        num_retries = self.request.retries
        seconds_to_wait = 2.0 ** num_retries
        raise self.retry(countdown=seconds_to_wait)
    else:
        if r.status_code == 200:
            results = r.json()['results']
            if not len(result) > 0:
                return False
            else:
                location = results[0]['geometry']['location']
                return write_geocode_to_db(url, location)


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
