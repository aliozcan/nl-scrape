FROM python:3.6.5-alpine3.7

RUN apk add --update --no-cache g++ gcc libxslt-dev
WORKDIR /fundanl
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
