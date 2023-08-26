from uuid import UUID

import pytest
from moto import mock_s3, mock_dynamodb, mock_sqs

from functions.render_trigger.src.handler import *


@pytest.fixture
def set_envs(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("S3_BUCKET", "EXAMPLE-BUCKET")
    monkeypatch.setenv("SQS_QUEUE", "EXAMPLE-QUEUE")


@pytest.fixture(scope="function")
def s3(set_envs):
    with mock_s3():
        bucket_name = "EXAMPLE-BUCKET"
        s3c = boto3.client("s3")
        s3c.create_bucket(Bucket=bucket_name)
        with open("resources/test-data.json", "rb") as test_file:
            s3c.put_object(Bucket=bucket_name, Key="EXAMPLE-PREFIX/example.json", Body=test_file,
                           ContentType="application/json")
        yield s3c


@pytest.fixture(scope="function")
def sqs(set_envs):
    with mock_sqs():
        sqsc = boto3.client("sqs")
        sqsc.create_queue(QueueName="EXAMPLE-QUEUE")
        yield sqsc


@pytest.fixture(scope="function")
def dynamo(set_envs):
    with mock_dynamodb():
        dbc = boto3.client("dynamodb", region_name="us-east-1")
        dbc.create_table(TableName='render_jobs',
                         AttributeDefinitions=[
                             {
                                 "AttributeName": "render_job_id",
                                 "AttributeType": "S"
                             },
                             {
                                 "AttributeName": "start_time",
                                 "AttributeType": "S"
                             }
                         ],
                         KeySchema=[
                             {
                                 "AttributeName": "render_job_id",
                                 "KeyType": "HASH"
                             },
                             {
                                 "AttributeName": "start_time",
                                 "KeyType": "RANGE"
                             }
                         ],
                         ProvisionedThroughput={
                             "ReadCapacityUnits": 1,
                             "WriteCapacityUnits": 1
                         }
                         )
        yield dbc


@pytest.fixture
def patch_time(monkeypatch):
    def mock_time():
        return 1

    monkeypatch.setattr(time, 'time', mock_time)


@pytest.fixture
def patch_uuid(monkeypatch):
    def mock_uuid():
        return UUID('12345678123456781234567812345678')

    monkeypatch.setattr(uuid, 'uuid4', mock_uuid)


def test_execute(s3, dynamo, sqs, patch_time, patch_uuid):
    with open("resources/s3-put-event.json") as file:
        event = json.load(file)

    result = execute(event, "")


    get_item_response = dynamo.get_item(TableName='render_jobs',
                                        Key={'render_job_id': {'S': '12345678-1234-5678-1234-567812345678'},
                                             'start_time': {'S': '1970-01-01 00:00:01'}})

    print(get_item_response)
    assert 'Item' in get_item_response.keys()
    assert result.file_name == "test/default_cube.blend"
    assert result.frames == 1
    assert result.output_name == "first_render"


def test_get_time(patch_time):
    actual_result = get_time()
    expected_result = "1970-01-01 00:00:01"
    assert expected_result == actual_result


def test_generate_job_meta(patch_time, patch_uuid):
    actual_result = generate_job_meta("somefile", "someoutputname")
    expected_full_output = 'render-output/somefile/1970-01-01_00-00-01/12345678/someoutputname'
    expected_id_db = UUID('12345678-1234-5678-1234-567812345678')
    expected_readable_time = "1970-01-01 00:00:01"
    assert actual_result.full_output_path == expected_full_output
    assert actual_result.id_db == expected_id_db
    assert actual_result.readable_time == expected_readable_time


def test_output_path():
    file_name = "test filename.blend"
    id_db = "c98d55ff-9880-45c2-8d1b-e34323bc6b8c"
    readable_time = "2023-08-20 00:07:46"
    expected_output_path = "render-output/test_filename/2023-08-20_00-07-46/c98d55ff/"

    actual_output_path = create_output_path(file_name, id_db, readable_time)
    assert expected_output_path == actual_output_path


def test_chunk():
    expected_chunks = [range(10, 20), range(20, 30), range(30, 35)]
    # [[10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
    # [20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
    # [30, 31, 32, 33, 34]]

    actual_chunks = chunks(range(10, 35), 10)

    assert expected_chunks == list(actual_chunks)


def test_path_friendly_filename():
    filename = 'some junk.blend'
    expected_output = 'some_junk'

    assert expected_output == path_friendly_filename(filename)

    filename = 'some other junk.BLEND'
    expected_output = 'some_other_junk'

    assert expected_output == path_friendly_filename(filename)

    filename = 'not a blend file'
    expected_output = 'not_a_blend_file'

    assert expected_output == path_friendly_filename(filename)
