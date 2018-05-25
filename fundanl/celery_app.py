import os
from celery import Celery
# from celery.schedules import crontab

app = Celery(
    'celery_app',
    broker=(f"amqp://"
            f"{os.getenv('RABBITMQ_DEFAULT_USER')}:"
            f"{os.getenv('RABBITMQ_DEFAULT_PASS')}@"
            f"rabbitmq:5672"),
    backend='rpc://',
    include=['scraper.tasks', 'geocoder.tasks']
)

app.conf.update(
    timezone='Europe/Amsterdam',
    result_expires=3600
)
