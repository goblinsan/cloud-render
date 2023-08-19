def execute(event, context):
    records = event['Records']
    for record in records:
        content = record['body']
        print(content)

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