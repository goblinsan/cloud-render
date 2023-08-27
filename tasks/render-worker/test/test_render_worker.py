import json
import os
from types import SimpleNamespace

import boto3
import pytest
from moto import mock_s3, mock_sqs

from render_worker import ensure_envvars, get_messages, extract_instructions_from_messages, extract_instruction, \
    render_frame, create_blender_command


@pytest.fixture
def set_envs(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
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

        with open("resources/default_cube.blend", "rb") as test_blend:
            s3c.put_object(Bucket=bucket_name, Key='some_blend_file.blend', Body=test_blend)

        yield s3c


@pytest.fixture(scope="function")
def sqs(set_envs):
    with mock_sqs():
        sqsc = boto3.client("sqs")
        sqsc.create_queue(QueueName="EXAMPLE-QUEUE")
        with open("resources/test_msg_entry.json") as file:
            entry = json.load(file)
        sqsc.send_message_batch(Entries=[entry], QueueUrl="EXAMPLE-QUEUE")
        yield sqsc


@pytest.fixture(scope="function")
def sample_render_instruction():
    s3_bucket = "EXAMPLE-BUCKET"
    object_name = "some/fake/path_00003"
    render_file = "some_blend_file.blend"
    render_fr = 3
    return SimpleNamespace(
        s3_bucket=s3_bucket, object_name=object_name, render_file=render_file, render_frame=render_fr
    )


def test_ensure_envvars(set_envs):
    ensure_envvars()


def test_render_frame(s3, sample_render_instruction, fp):
    # Mock subprocess
    current_dir = os.getcwd()
    blender_command = ["/bin/blender/3.6.2/blender", "-b", "file.blend", "-o", f"{current_dir}\\output_file_", "-f",
                       "3"]
    fp.register(blender_command)

    # Create dummy rendered output file
    f = open('output_file_0003.png', 'w')
    f.write('fake file')
    f.close()

    render_frame(s3, sample_render_instruction)

    # Assert output is written back to bucket
    s3.get_object(
        Bucket='EXAMPLE-BUCKET',
        Key='some/fake/path_00003.png'
    )

    # delete files created during this test
    os.remove('file.blend')
    os.remove('output_file_0003.png')


def test_get_messages(sqs):
    with open("resources/test_messages.json") as file:
        expected_messages = json.load(file)
    actual_messages = get_messages(sqs)

    assert expected_messages['Messages'][0]['Body'] == actual_messages['Messages'][0]['Body']
    assert expected_messages['Messages'][0]['MessageAttributes'] == actual_messages['Messages'][0]['MessageAttributes']


def test_extract_instructions_from_messages(sqs, sample_render_instruction):
    response = sqs.receive_message(
        QueueUrl='EXAMPLE-QUEUE',
        MessageAttributeNames=['All'],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=1
    )

    expected_instructions = [sample_render_instruction]

    assert extract_instructions_from_messages(response['Messages'], sqs) == expected_instructions


def test_extract_instruction(sample_render_instruction):
    with open("resources/test_messages.json") as file:
        test_messages = json.load(file)

    assert extract_instruction(test_messages['Messages'][0]) == sample_render_instruction


def test_create_blender_command(sample_render_instruction):
    current_dir = os.getcwd()
    expected_output = ["/bin/blender/3.6.2/blender", "-b", "file.blend", "-o", f"{current_dir}\\output_file_", "-f",
                       "3"]
    actual_output = create_blender_command("/bin/blender/3.6.2/blender", current_dir, sample_render_instruction)
    assert actual_output == expected_output
