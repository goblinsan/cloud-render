import json
import time
import urllib.parse
import uuid

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
        fileName = body_read["fileName"]
        frames = body_read["frames"]
        outputName = body_read["outputName"]

        id_db = uuid.uuid4()
        ts = time.time()
        dynamo.put_item(TableName='render-jobs',
                        Item={
                            'render-job-id': {'S': str(id_db)},
                            'startTime': {'S': str(ts)},
                            'secondsToExpire': {'N': '300'},  # 5 minutes
                            'fileName': {'S': fileName},
                            'frames': {'N': str(frames)},
                            'outputName': {'S': outputName}})

        queue_url = 'https://sqs.us-east-2.amazonaws.com/056985368977/render-queue'
        for i in range(frames):
            sqs.send_message(
                QueueUrl=queue_url,
                MessageAttributes={
                    'RenderJobId': {
                        'DataType': 'String',
                        'StringValue': str(id_db)
                    },
                    'File': {
                        'DataType': 'String',
                        'StringValue': fileName
                    },
                    'Frame': {
                        'DataType': 'Number',
                        'StringValue': str(i + 1)
                    }
                },
                MessageBody=(
                    'Render frame :' + str(i + 1)
                )
            )

        return body_read
    except Exception as e:
        print(e)
        print(
            'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(
                key, bucket))
        raise e