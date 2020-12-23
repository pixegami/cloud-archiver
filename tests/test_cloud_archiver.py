import time
import uuid
import os
import shutil
from datetime import datetime, timedelta
from unittest.mock import Mock

from src.cloud_archiver import CloudArchiver

OUTPUT_PATH = "test_output"
SAMPLE_DATA_PATH = os.path.join(OUTPUT_PATH, "sample_data")
ARCHIVE_PATH = os.path.join(OUTPUT_PATH, "archive")


def setup_module():
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    os.makedirs(SAMPLE_DATA_PATH, exist_ok=True)
    os.makedirs(ARCHIVE_PATH, exist_ok=True)


def teardown_module():
    shutil.rmtree(OUTPUT_PATH)


def test_traverse():
    # Test that we can fully traverse the directory and figure out the timestamp of each root node.
    archiver = CloudArchiver()
    generate_test_set()

    result = archiver.traverse(SAMPLE_DATA_PATH, 1)
    n_root_paths = sum([1 for _, x in result.items() if x.is_root])
    n_archive_files = sum([1 for _, x in result.items() if x.should_archive and not x.is_dir])

    # Print the result.
    archiver.display_paths(SAMPLE_DATA_PATH, result)
    assert(n_root_paths == 12)  # Expect 12 root paths.
    assert(n_archive_files == 7)  # Expect these many files to be archived.


def test_archive():
    archiver = CloudArchiver()
    generate_test_set()

    result = archiver.traverse(SAMPLE_DATA_PATH, 1)
    items = archiver.transfer_to_archive(result, ARCHIVE_PATH)
    assert (len(items) == 7)  # Expect these many files to be archived.


def test_upload():
    archiver = CloudArchiver()

    def fake_upload(path, bucket, key):
        print(f"Uploading [{path}, {bucket}, {key}]")
        time.sleep(0.5)

    mock_s3_client = Mock()
    archiver.get_s3_client = Mock(return_value=mock_s3_client)
    mock_s3_client.create_bucket = Mock()
    mock_s3_client.upload_file = Mock(side_effect=fake_upload)

    key_prefix = "test-archive"
    bucket = "archive.bucket"
    generate_test_set()

    result = archiver.traverse(SAMPLE_DATA_PATH, 1)
    items = archiver.transfer_to_archive(result, ARCHIVE_PATH)
    archiver.upload(bucket, key_prefix, items)


def generate_test_set():
    # Generate some files in the base directory.
    generate_test_files(5, SAMPLE_DATA_PATH, days_old=0)
    generate_test_files(5, SAMPLE_DATA_PATH, days_old=3)

    # Generate a directory with some files.
    # This should NOT be archived.
    test_dir_1 = generate_directory(SAMPLE_DATA_PATH, "test_dir_1")
    generate_test_files(4, test_dir_1, days_old=0)
    generate_test_files(1, test_dir_1, days_old=6)  # Even though these are old, the folder was touched recently.

    # Generate a directory. This one has no files, but has a nested dir with some old files.
    # These should be archived.
    test_dir_2 = generate_directory(SAMPLE_DATA_PATH, "test_dir_2")
    nested_dir_1 = generate_directory(test_dir_2, "nested_dir_1")
    generate_test_files(2, nested_dir_1, days_old=6)


def generate_directory(root_path: str, directory_name: str):
    directory_path = os.path.join(root_path, directory_name)
    os.makedirs(directory_path, exist_ok=True)
    return directory_path


def generate_test_files(n: int, root_path: str, days_old: int = 0):

    for _ in range(n):
        unique_id = uuid.uuid4().hex[:5]
        random_name = f"file_{unique_id}_{days_old}d.txt"
        file_path = os.path.join(root_path, random_name)
        with open(file_path, "w") as f:
            f.write("Random text file created for testing.")
        edit_date = datetime.now() - timedelta(days=days_old)
        os.utime(file_path, (edit_date.timestamp(), edit_date.timestamp()))