import unittest

from moto import mock_s3

from functions.render_trigger.src.handler import *


class TestExecuteHandler(unittest.TestCase):

    bucket_name = "EXAMPLE-BUCKET"
    def setUp(self):
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket_name)
        bucket.create()

        s3_client = boto3.client("s3", region_name="us-east-1")

        with open("resources/test-data.json", "rb") as test_file:
            s3_client.put_object(Bucket=self.bucket_name, Key="EXAMPLE-PREFIX/example.json", Body=test_file, ContentType="application/json")

    def test_execute(self):
        with open("resources/s3-put-event.json") as file:
            event = json.load(file)

        result = execute(event, "")

        self.assertEqual(result["fileName"], "someProject/fakeFile.blend")
        self.assertEqual(result["frames"], 123)
        self.assertEqual(result["outputName"], "makeItThisName")


    def test_output_path(self):
        file_name = "test filename.blend"
        id_db = "c98d55ff-9880-45c2-8d1b-e34323bc6b8c"
        readable_time = "2023-08-20 00:07:46"
        expected_output_path = "render-output/test_filename/2023-08-20_00-07-46/c98d55ff/"

        actual_output_path = create_output_path(file_name, id_db, readable_time)
        self.assertEqual(expected_output_path, actual_output_path)


    def test_chunk(self):
        expected_chunks = [range(10, 20), range(20, 30), range(30, 35)]
        # [[10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        # [20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
        # [30, 31, 32, 33, 34]]

        actual_chunks = chunks(range(10, 35),10)

        self.assertEqual(expected_chunks, list(actual_chunks))


    def test_path_friendly_filename(self):
        filename = 'some junk.blend'
        expected_output = 'some_junk'

        self.assertEqual(expected_output, path_friendly_filename(filename))

        filename = 'some other junk.BLEND'
        expected_output = 'some_other_junk'

        self.assertEqual(expected_output, path_friendly_filename(filename))

        filename = 'not a blend file'
        expected_output = 'not_a_blend_file'

        self.assertEqual(expected_output, path_friendly_filename(filename))




if __name__ == '__main__':
    unittest.main()