import os
import psycopg2
import logging
import json
from itertools import chain
from typing import Dict, Tuple, List
from celery.utils.log import get_task_logger


logger = get_task_logger('db')


_connection = (f"host=postgres "
               f"dbname=postgres "
               f"user={os.getenv('POSTGRES_USER')} "
               f"password={os.getenv('POSTGRES_PASSWORD')}")
conn = psycopg2.connect(_connection)


def get_unique_urls_by_query(sql: str) -> set:
    try:
        cur = conn.cursor()
        cur.execute(sql)
        res = cur.fetchall()
    except Exception as e:
        logger.info(f'{e}')
        res = []
    finally:
        cur.close()
        return res


def get_missing_url_coordinates() -> List[Tuple[str, str]]:
    sql = ("select url, body->>'Address' "
           'from fundanl '
           'where geometry is null '
           'limit 1500;')
    return get_unique_urls_by_query(sql)


def get_scraped_results() -> set:
    sql = 'select distinct url from fundanl;'
    res = get_unique_urls_by_query(sql)
    res = set(chain(*res))
    return res


def init_db():
    try:
        cur = conn.cursor()
        cur.execute("""create table if not exists fundanl(
                        url text,
                        body json,
                        geometry geometry(Point, 4326));""")
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cur.close()


def execute_sql(sql: str, *args) -> bool:
    try:
        cur = conn.cursor()
        #logger.info(f'{sql} with {args}')
        cur.execute(sql, args)
        conn.commit()
    except Exception as e:
        logger.info(f'{e}')
        conn.rollback()
    else:
        logger.info(f'{sql} with {args} is done')
    finally:
        cur.close()


def write_results_to_db(url: str, geometry: Tuple, house: Dict) -> bool:
    sql = "insert into fundanl(url, body, geometry) values (%s, %s, st_setsrid(st_makepoint(%s, %s), 4326))"
    values = (url, json.dumps(house), geometry[0], geometry[1])
    return execute_sql(sql, *values)


def write_geocode_to_db(url: str, coords: Dict) -> bool:
    sql = """update fundanl
            set geometry = ST_PointFromText('POINT(%s %s)', 4326)
            where url = %s"""
    values = (coords['lng'], coords['lat'], url)
    return execute_sql(sql, *values)
