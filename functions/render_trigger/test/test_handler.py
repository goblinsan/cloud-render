import json
import unittest

import boto3
from moto import mock_s3
from functions.render_trigger.src.handler import execute


class TestExecuteHandler(unittest.TestCase):

    bucket_name = "EXAMPLE-BUCKET"
    def setUp(self):
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket_name)
        bucket.create()

        s3_client = boto3.client("s3", region_name="us-east-1")

        with open("resources/test-date.json", "rb") as test_file:
            s3_client.put_object(Bucket=self.bucket_name, Key="EXAMPLE-PREFIX/example.json", Body=test_file, ContentType="application/json")

    def test_execute(self):
        with open("resources/s3-put-event.json") as file:
            event = json.load(file)

        result = execute(event, "")

        self.assertEqual(result, "application/json")



if __name__ == '__main__':
    unittest.main()