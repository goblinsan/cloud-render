import json
import boto3

print('Loading function')


def execute(event, context):
    records = event['Records']
    for record in records:
        render_instruction = json.loads(record['body'])
        print("message is : ")
        print(str(record))
        print("body is : ")
        print(str(render_instruction))

        dummy_data = "I'm a render! #" + render_instruction['frame']
        encoded_dummy_data = dummy_data.encode("utf-8")
        bucket_name = render_instruction['s3_bucket']
        s3_path = render_instruction['object_name']

        s3 = boto3.resource("s3")
        s3.Bucket(bucket_name).put_object(Key=s3_path, Body=encoded_dummy_data)

# {"frame": "1", "s3_bucket": "sunset-ave-render-buckets", "object_name": "render-output/someproject/fakefile/2023-08-20_01-42-10/a53eee05/makeitthisname_00001"}

# Expected Message Format
# "MessageBody": "Render frame :" + str(i + 1),
# "MessageAttributes": {
#     'RenderJobId': {
#         'DataType': 'String',
#         'StringValue': str(id_db)
#     },
#     'File': {
#         'DataType': 'String',
#         'StringValue': file_name
#     },
#     'Frame': {
#         'DataType': 'Number',
#         'StringValue': str(i + 1)
#     }}
# }
