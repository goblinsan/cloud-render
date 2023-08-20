import json
import os
import boto3
import logging

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)


def ensure_envvars():
    """Ensure that these environment variables are provided at runtime"""
    required_envvars = [
        "AWS_REGION",
        "SQS_QUEUE"
    ]

    missing_envvars = []
    for required_envvar in required_envvars:
        if not os.environ.get(required_envvar, ''):
            missing_envvars.append(required_envvar)

    if missing_envvars:
        message = "Required environment variables are missing: " + \
                  repr(missing_envvars)
        raise AssertionError(message)


def process_message(message_body):
    logger.info(f"Processing message: {message_body}")

    render_instruction = json.loads(message_body)
    logger.info(f"render_instruction: {render_instruction}")

    dummy_data = "ECS WORKER SAYS: I'm a render! #" + render_instruction['frame']
    encoded_dummy_data = dummy_data.encode("utf-8")
    bucket_name = render_instruction['s3_bucket']
    s3_path = render_instruction['object_name']

    s3 = boto3.resource("s3")
    s3.Bucket(bucket_name).put_object(Key=s3_path, Body=encoded_dummy_data)
    pass


def main():
    logger.info("SQS Consumer starting ...")
    try:
        ensure_envvars()
    except AssertionError as e:
        logger.error(str(e))
        raise

    queue_name = os.environ["SQS_QUEUE"]
    logger.info(f"Subscribing to queue {queue_name}")
    sqs = boto3.resource("sqs")
    queue = sqs.get_queue_by_name(QueueName=queue_name)

    while True:
        messages = queue.receive_messages(
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1
        )
        for message in messages:
            try:
                process_message(message.body)
            except Exception as e:
                print(f"Exception while processing message: {repr(e)}")
                continue

            message.delete()


if __name__ == "__main__":
    main()