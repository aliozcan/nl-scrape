import os
import psycopg2
import logging
import json
from itertools import chain
from typing import Dict, Tuple


_connection = (f"host=postgres "
               f"dbname=postgres "
               f"user={os.getenv('POSTGRES_USER')} "
               f"password={os.getenv('POSTGRES_PASSWORD')}")
conn = psycopg2.connect(_connection)
logger = logging.getLogger(__name__)


def get_unique_urls_by_query(sql: str) -> set:
    try:
        cur = conn.cursor()
        cur.execute(sql)
        res = cur.fetchall()
        res = set(chain(*res))
    except Exception as e:
        logger.info(f'{e}')
        res = set()
    finally:
        cur.close()
        return res


def get_missing_url_coordinates() -> set:
    sql = ('select distinct url '
           'from fundanl '
           'where lat is null or lon is null '
           'limit 1500;')
    return get_unique_urls_by_query(sql)


def get_scraped_results() -> set:
    sql = 'select distinct url from fundanl;'
    return get_unique_urls_by_query(sql)


def init_db():
    try:
        cur = conn.cursor()
        cur.execute("""create table if not exists fundanl(
                        url text,
                        body json,
                        lat text,
                        lon text);""")
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cur.close()


def execute_sql(sql: str, *args) -> bool:
    try:
        cur = conn.cursor()
        cur.execute(sql, args)
        conn.commit()
    except Exception as e:
        logger.info(f'{e}')
        conn.rollback()
        return False
    else:
        return True
    finally:
        cur.close()


def write_results_to_db(url: str, house: Dict) -> bool:
    sql = "insert into fundanl(url, body) values (%s, %s)"
    values = (url, json.dumps(house))
    return execute_sql(sql, *values)


def write_geocode_to_db(url: str, coords: Tuple) -> bool:
    sql = """update fundanl
            set lat = %s, lon = %s
            where url = %s"""
    values = (url, coords[0], coords[1])
    return execute_sql(sql, values)


def close_connection():
    conn.close()
