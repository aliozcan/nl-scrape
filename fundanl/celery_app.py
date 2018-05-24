import os
from celery import Celery
# from celery.schedules import crontab

app = Celery(
    'fundanl',
    broker=f"amqp://{os.getenv('RABBITMQ_DEFAULT_USER')}:{os.getenv('RABBITMQ_DEFAULT_PASS')}@rabbitmq:5672",
    backend='rpc://',
    include=['scrape.tasks', 'geocode.tasks']
)

app.conf.update(
    timezone = 'Europe/Amsterdam'
)


'''
@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Executes every Monday morning at 7:30 a.m.
    sender.add_periodic_task(
        crontab(hour=7, minute=30, day_of_week=1),
        test.s('Happy Mondays!'),
    )
'''