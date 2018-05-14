Scraping funda.nl with celery, rabbitmq, rotating proxy and docker

There should be *.env* file in the main directory which contains the following:

```bash
POSTGRES_USER=user
POSTGRES_PASSWORD=pass
POSTGRES_DB=db
RABBITMQ_DEFAULT_USER=rabbituser
RABBITMQ_DEFAULT_PASS=rabbitpass
CELERY_BROKER_URL=amqp://rabbituser:rabbitpass@rabbitmq:5672//
FLOWER_USER=floweruser
FLOWER_PASS=flowerpass
```