import os
from celery import Celery


app = Celery(
    'fundanl',
    broker=f"amqp://{os.getenv('RABBITMQ_DEFAULT_USER')}:{os.getenv('RABBITMQ_DEFAULT_PASS')}@rabbitmq:5672",
    backend='rpc://',
    include=['scrape']
)
