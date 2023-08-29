import glob
import json
import os
import re
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


def put_render_in_s3(instruction, s3):
    current_dir = os.getcwd()
    try:
        path = f"{current_dir}\\output_file_*"
        for filename in glob.glob(path):
            f_name, extension = os.path.splitext(filename)
            output_with_extension = instruction.object_name + extension
            with open(filename, "rb") as test_blend:
                s3.put_object(Bucket=instruction.s3_bucket, Key=output_with_extension, Body=filename)
    except ClientError as err:
        deal_with_error(err)


def create_blender_command(instruction, gpu_script_path, gpu_flag, gpu_name):
    blender_path = os.environ.get('BLENDER_PATH', "/bin/blender/3.6.2/blender")
    current_dir = os.getcwd()
    gpu_script = "render_with_gpu.py"
    if gpu_script_path:
        gpu_script = gpu_script_path + gpu_script
    base_command = [blender_path,
                    "-b", "file.blend",
                    "-o", f"{current_dir}\\output_file_",
                    "-P", gpu_script,
                    "-f", str(instruction.render_frame)]
    if gpu_flag:
        base_command.extend(["--", gpu_name])

    return base_command


def render_frame(blender_command):
    logger.info(f"Running this Blender Command : {blender_command}")

    try:
        subprocess.run(blender_command, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(e.stderr)


def save_blend_file_locally(instruction, s3):
    try:
        s3.download_file(instruction.s3_bucket, instruction.render_file, './file.blend')
    except ClientError as err:
        deal_with_error(err)


def select_best_gpu(gpu_list):
    gpu_names = []
    for gpu in gpu_list:
        m = re.search("GPU [0-9]+: (.+?) \(UUID", gpu)
        if m:
            gpu_names.append(m.group(1))
        else:
            logger.error("Couldn't find the GPU name returned from nvidia-smi")
    rtx_gpu_filter = filter(lambda a: 'RTX' in a, gpu_names)
    rtx_list = list(rtx_gpu_filter)
    if rtx_list:
        selected_gpu = rtx_list[0]
    else:
        selected_gpu = gpu_names[0]

    return selected_gpu


def use_gpu():
    try:
        cmd_output = subprocess.check_output(['nvidia-smi', '-L'])
        logger.info('Nvidia GPU detected!')
        gpu_list = cmd_output.decode('utf-8').strip().split('\n')
        selected_gpu = select_best_gpu(gpu_list)
        return True, selected_gpu
    except Exception as e:
        logger.info('No Nvidia GPU detected or Error Selecting GPU: ' + str(e))
        return False, None


def process_instruction(gpu_flag, gpu_name, instruction, s3):
    save_blend_file_locally(instruction, s3)
    blender_cmd = create_blender_command(instruction, None, gpu_flag, gpu_name)
    render_frame(blender_cmd)
    put_render_in_s3(instruction, s3)


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
            logger.error(f"Exception while processing message: {repr(e)}")
            continue
        else:
            sqs.delete_message(
                QueueUrl=os.environ["SQS_QUEUE"],
                ReceiptHandle=receipt_handle
            )

    return instructions


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


def get_aws_clients():
    s3 = boto3.resource("s3")
    sqs = boto3.client("sqs")
    return s3, sqs


def deal_with_error(err):
    if err.response['Error']['Code'] == 'InternalError':
        logger.error('Error Message: {}'.format(err.response['Error']['Message']))
        logger.error('Request ID: {}'.format(err.response['ResponseMetadata']['RequestId']))
        logger.error('Http code: {}'.format(err.response['ResponseMetadata']['HTTPStatusCode']))
    else:
        raise err


def main():
    logger.info("Render Worker starting ...")

    s3, sqs = get_aws_clients()

    try:
        ensure_envvars()
    except AssertionError as e:
        logger.error(str(e))
        raise

    response = get_messages(sqs)
    instructions = extract_instructions_from_messages(response['Messages'], sqs)
    if instructions:
        gpu_flag, gpu_name = use_gpu()
        for instruction in instructions:
            process_instruction(gpu_flag, gpu_name, instruction, s3)


if __name__ == "__main__":
    main()
