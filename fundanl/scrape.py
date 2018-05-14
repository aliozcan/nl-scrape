import datetime as dt
from collections import OrderedDict
from requests_html import HTMLResponse
from celery_app import app
from db import write_results_to_db
from celery.utils.log import get_task_logger
from proxy import get_session



logger = get_task_logger(__name__)


@app.task(bind=True, max_retries=3, default_retry_delay=10, task_time_limit=60, task_soft_time_limit=50)
def scrape(self, house_url: str) -> HTMLResponse:
    session = get_session()
    try:
        logger.info(f'Scraping {house_url}')
        r = session.get(house_url, timeout=5)
    except Exception as e:
        logger.info(f'Exception in {house_url}: {e}')
        num_retries = self.request.retries
        seconds_to_wait = 2.0 ** num_retries
        raise self.retry(countdown=seconds_to_wait)
    else:
        if r.status_code == 200:
            parse_page(r, house_url)


def parse_page(r: HTMLResponse, url: str) -> OrderedDict:
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
    write_results_to_db(url, house)
