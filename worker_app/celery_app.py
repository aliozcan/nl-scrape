import os
import time
import random
from celery import Celery


app = Celery(
    'celery_app',
    broker = f"amqp://{os.getenv('RABBITMQ_DEFAULT_USER')}:{os.getenv('RABBITMQ_DEFAULT_PASS')}@rabbitmq:5672",
    backend = 'rpc://'
)

@app.task(bind=True, default_retry_delay=10)
def do_work(self, item):
    print('Task received ' + str(item))
    # sleep for random seconds to simulate a really long task
    time.sleep(random.randint(1, 3))

    result = item + item
    return result

if __name__ == '__main__':
    for i in range(10):
        result = do_work.delay(i)
        print('task submitted' + str(i))