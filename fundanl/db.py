import os
import psycopg2
import logging
import json
from itertools import chain
from collections import ChainMap, OrderedDict

_connection = f"host=postgres user={os.getenv('POSTGRES_USER')} password={os.getenv('POSTGRES_PASSWORD')}"
conn = psycopg2.connect(_connection)
logger = logging.getLogger(__name__)

def get_scraped_results() -> set:
    try:
        cur = conn.cursor()
        cur.execute('select distinct url from fundanl')
        existing_houses = cur.fetchall()
        existing_houses = set(chain(*existing_houses))
    except Exception as :
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


def write_results_to_db(url:str, house:OrderedDict):
    try:
        cur = conn.cursor()
        cur.execute("insert into fundanl(url, body) values (%s, %s)", (url, json.dumps(house)))
        conn.commit()    
    except Exception as e:
        logger.info(f'{e}')
        conn.rollback()
    else:
        logger.info(f'{url} written.')
    finally:
        cur.close()