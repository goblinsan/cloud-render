import json
import time
import urllib.parse
import uuid
from datetime import datetime

import boto3
import os

S3_BUCKET = os.environ['S3_BUCKET']
SQS_QUEUE = os.environ['SQS_QUEUE']

print('Loading function')

s3 = boto3.client('s3')
dynamo = boto3.client('dynamodb')
sqs = boto3.client('sqs')


def execute(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        body_read = json.loads(response['Body'].read())
        file_name = body_read["file_name"]
        frames = body_read["frames"]
        output_name = body_read["output_name"]
        output_name = path_friendly_filename(output_name)
        id_db = uuid.uuid4()
        ts = time.time()
        readable_time = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        full_output_path = create_output_path(file_name, id_db, readable_time) + output_name

        dynamo.put_item(TableName='render_jobs',
                        Item=create_db_item(file_name, frames, id_db, full_output_path, readable_time))

        queue_url = SQS_QUEUE
        entries = []
        for i in range(frames):
            entry = create_sqs_entry(file_name, i, id_db, full_output_path)
            entries.append(entry)

        max_sqs_batch_size = 10
        response = []
        for batch in chunks(entries, max_sqs_batch_size):
            response.extend(sqs.send_message_batch(Entries=batch, QueueUrl=queue_url))

        return body_read
    except Exception as e:
        print(e)
        print(
            'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(
                key, bucket))
        raise e


def create_sqs_entry(file_name, i, id_db, full_output_path):
    entry = {"Id": str(i),
             "MessageBody": create_message_body(i, full_output_path),
             "MessageAttributes": {
                 'RenderJobId': {
                     'DataType': 'String',
                     'StringValue': str(id_db)
                 },
                 'File': {
                     'DataType': 'String',
                     'StringValue': file_name
                 },
                 'Frame': {
                     'DataType': 'Number',
                     'StringValue': str(i + 1)
                 }}
             }
    return entry


def create_message_body(i, full_output_path):
    frame_as_string = str(i + 1)
    padded_frame_as_string = str(i + 1).zfill(5)
    message_body = {
        'frame': frame_as_string,
        's3_bucket': S3_BUCKET,
        'object_name': full_output_path + '_' + padded_frame_as_string
    }
    return json.dumps(message_body)


def create_db_item(file_name, frames, id_db, full_output_path, readable_time):
    return {
        'render_job_id': {'S': str(id_db)},
        'start_time': {'S': str(readable_time)},
        'seconds_to_expire': {'N': '300'},  # 5 minutes
        'file_name': {'S': file_name},
        'frames': {'N': str(frames)},
        'output_name': {'S': full_output_path}}


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def create_output_path(file_name, id_db, readable_time):
    path_friendly_time = readable_time.replace(' ', '_').replace(':', '-')
    return f"render-output/{path_friendly_filename(file_name)}/{path_friendly_time}/{str(id_db)[0:8]}/"


def path_friendly_filename(file_name):
    blend_extension = '.blend'
    file_name = str.lower(file_name).split(blend_extension)[0]

    return file_name.replace(' ', '_')
