import glob
import json
import os
import subprocess
from types import SimpleNamespace

import boto3
import logging

from botocore.exceptions import ClientError

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)


def ensure_envvars():
    """Ensure that these environment variables are provided at runtime"""
    required_envvars = [
        "AWS_REGION",
        "S3_BUCKET",
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


def create_blender_command(blender_path, output_path, instruction):
    return [blender_path, "-b", "file.blend", "-o", f"{output_path}\\output_file_", "-f", str(instruction.render_frame)]


def render_frame(s3, instruction):
    logger.info(f"Rendering : {instruction.render_file} Frame: {instruction.render_frame}")

    try:
        s3.download_file(instruction.s3_bucket, instruction.render_file, './file.blend')
    except ClientError as err:
        deal_with_error(err)

    # path in docker is "/bin/blender/3.6.2/blender"
    blender_path = os.environ.get('BLENDER_PATH', "/bin/blender/3.6.2/blender")
    current_dir = os.getcwd()

    blender_command = create_blender_command(blender_path, current_dir, instruction)
    try:
        subprocess.run(blender_command, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(e.stderr)

    # put the resulting file in the output bucket
    try:
        path = f"{current_dir}\\output_file_*"
        for filename in glob.glob(path):
            f_name, extension = os.path.splitext(filename)
            output_with_extension = instruction.object_name + extension
            with open(filename, "rb") as test_blend:
                s3.put_object(Bucket=instruction.s3_bucket, Key=output_with_extension, Body=filename)
    except ClientError as err:
        deal_with_error(err)


def deal_with_error(err):
    if err.response['Error']['Code'] == 'InternalError':
        print('Error Message: {}'.format(err.response['Error']['Message']))
        print('Request ID: {}'.format(err.response['ResponseMetadata']['RequestId']))
        print('Http code: {}'.format(err.response['ResponseMetadata']['HTTPStatusCode']))
    else:
        raise err


def get_aws_clients():
    s3 = boto3.resource("s3")
    sqs = boto3.client("sqs")
    return s3, sqs


def get_messages(sqs):
    logger.info(f"SQS Consumer starting for : {os.environ['SQS_QUEUE']}")
    try:
        response = sqs.receive_message(
            QueueUrl=os.environ["SQS_QUEUE"],
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1
        )
    except ClientError as error:
        logger.exception("Couldn't receive messages from queue: %s", os.environ["SQS_QUEUE"])
        raise error
    else:
        return response


def extract_instruction(message):
    message_body = json.loads(message['Body'])
    s3_bucket = message_body['s3_bucket']
    object_name = message_body['object_name']
    render_file = message['MessageAttributes']['Render_File']['StringValue']
    render_frame = int(message['MessageAttributes']['Render_Frame']['StringValue'])
    return SimpleNamespace(
        s3_bucket=s3_bucket, object_name=object_name, render_file=render_file, render_frame=render_frame
    )


def extract_instructions_from_messages(messages, sqs):
    instructions = []
    for message in messages:
        logger.info(f"Received : {message}")
        receipt_handle = message['ReceiptHandle']
        logger.info(f"message Body is : {message['Body']}")
        try:
            logger.info(f"try to extract this message body: {message}")
            instructions.append(extract_instruction(message))
        except Exception as e:
            print(f"Exception while processing message: {repr(e)}")
            continue
        else:
            sqs.delete_message(
                QueueUrl=os.environ["SQS_QUEUE"],
                ReceiptHandle=receipt_handle
            )

    return instructions


def main():
    logger.info("Render Worker starting ...")

    s3, sqs = get_aws_clients()

    try:
        ensure_envvars()
    except AssertionError as e:
        logger.error(str(e))
        raise

    response = get_messages(sqs)
    extract_instructions_from_messages(response['Messages'], sqs)


if __name__ == "__main__":
    main()
