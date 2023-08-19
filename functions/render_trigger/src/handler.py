import json
import time
import urllib.parse
import uuid
from datetime import datetime

import boto3

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
        print(f"Response Body: {body_read}")
        file_name = body_read["file_name"]
        frames = body_read["frames"]
        output_name = body_read["output_name"]

        id_db = uuid.uuid4()
        ts = time.time()
        readable_time = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        dynamo.put_item(TableName='render_jobs',
                        Item={
                            'render_job_id': {'S': str(id_db)},
                            'start_time': {'S': str(readable_time)},
                            'seconds_to_expire': {'N': '300'},  # 5 minutes
                            'file_name': {'S': file_name},
                            'frames': {'N': str(frames)},
                            'output_name': {'S': output_name}})

        queue_url = 'https://sqs.us-east-2.amazonaws.com/056985368977/render-queue'
        entries = []
        for i in range(frames):
            entry = {"Id": str(i),
                     "MessageBody": "Render frame :" + str(i + 1),
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


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

# https://stackoverflow.com/questions/312443/how-do-i-split-a-list-into-equally-sized-chunks/312464#312464
# import pprint
# pprint.pprint(list(chunks(range(10, 75), 10)))
# [[10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
#  [20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
#  [30, 31, 32, 33, 34, 35, 36, 37, 38, 39],
#  [40, 41, 42, 43, 44, 45, 46, 47, 48, 49],
#  [50, 51, 52, 53, 54, 55, 56, 57, 58, 59],
#  [60, 61, 62, 63, 64, 65, 66, 67, 68, 69],
#  [70, 71, 72, 73, 74]]
