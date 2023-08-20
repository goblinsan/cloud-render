FROM python:3.11-slim-bookworm

WORKDIR /app

COPY ./src /app

RUN pip install -r requirements.txt

ENV SQS_QUEUE=https://sqs.us-east-2.amazonaws.com/056985368977/render-queue

ENTRYPOINT python sqs_consumer.py
