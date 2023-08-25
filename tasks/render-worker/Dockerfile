FROM python:3.11-slim-bookworm

ADD blender_binary/blender-3.6.2-linux-x64.tar.xz /bin/blender/3.6.2/

ENV BLENDER_PATH ="/bin/blender/3.6.2/"

WORKDIR /app

COPY ./src /app

RUN pip install -r requirements.txt

ENV SQS_QUEUE=https://sqs.us-east-2.amazonaws.com/056985368977/render-queue

ENTRYPOINT python render_worker.py
