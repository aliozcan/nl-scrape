from celery.utils.log import get_task_logger
from fundanl.proxy import get_session
from fundanl.celery_app import app
from fundanl.db import write_geocode_to_db


logger = get_task_logger(__name__)


@app.task(bind=True, max_retries=3, default_retry_delay=10,
          task_time_limit=60, task_soft_time_limit=50)
def geocode(self, url: str, address: str) -> bool:
    session = get_session(use_proxy=False, default_header=True)
    url = 'https://maps.googleapis.com/maps/api/geocode/json'
    params = {'sensor': 'false',
              'address': f'{address}',
              'region': 'nl'}
    session.params = params
    try:
        logger.info(f'geocoding {url}')
        r = session.get(url, timeout=5)
    except Exception as e:
        num_retries = self.request.retries
        seconds_to_wait = 2.0 ** num_retries
        raise self.retry(countdown=seconds_to_wait)
    else:
        if r.status_code == 200:
            try:
                results = r.json()['results']
                if not len(results) > 0:
                    raise Exception('Result length is 0')
                elif results['status'] == 'OVER_QUERY_LIMIT':
                    raise Exception('Query Limit Exceed')
                else:
                    location = results[0]['geometry']['location']
                    return write_geocode_to_db(url, location)
            except KeyError as e:
                logger.info(f'Wrong keys for dict. {r.text}')
            except Exception as e:
                # retry sometime tomorrow
                raise self.retry(countdown=90000)
        else:
            logger.info(f'status code {r.status_code}. {url}')
            raise ValueError('status code is not 200')
