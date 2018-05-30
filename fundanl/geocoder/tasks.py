from celery.utils.log import get_task_logger
from fundanl.proxy import get_session
from fundanl.celery_app import app
from fundanl.db import write_geocode_to_db


logger = get_task_logger(__name__)


@app.task(bind=True, max_retries=3, default_retry_delay=10,
          task_time_limit=60, task_soft_time_limit=50)
def geocode(self, url: str, address: str) -> bool:
    session = get_session(use_proxy=False, default_header=True)
    rurl = 'https://maps.googleapis.com/maps/api/geocode/json'
    params = {'sensor': 'false',
              'address': f'{address}',
              'region': 'nl'}
    session.params = params
    try:
        logger.info(f'geocoding {url}')
        r = session.get(rurl, timeout=5)
        if r.status_code == 200:
            result = r.json()
            if result['status'] == 'OK' and len(result['results']) > 0:
                results = result['results']
                location = results[0]['geometry']['location']
                return write_geocode_to_db(url, location)
    except Exception as e:
        logger.info(f'{e}')
        raise self.retry(countdown=90000)
