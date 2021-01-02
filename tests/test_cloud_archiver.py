import time
import os
import shutil
from unittest.mock import Mock

from src.cloud_archiver.analyze_directory import analyze_directory
from src.cloud_archiver.display_archive_items import display_archive_items
from src.cloud_archiver.display_paths import display_paths
from src.cloud_archiver.file_generator import generate_test_set
from src.cloud_archiver.get_items_in_archive import get_items_in_archive
from src.cloud_archiver.transfer_to_archive import transfer_to_archive
from src.cloud_archiver.upload_archive import upload

OUTPUT_PATH = "test_output"
SAMPLE_DATA_PATH = os.path.join(OUTPUT_PATH, "sample_data")
ARCHIVE_PATH = os.path.join(OUTPUT_PATH, "archive")
ARCHIVE_FOLDER = ".archive"
IGNORE_PATHS = [ARCHIVE_FOLDER]


def setup():
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    os.makedirs(SAMPLE_DATA_PATH, exist_ok=True)
    os.makedirs(ARCHIVE_PATH, exist_ok=True)


def teardown():
    shutil.rmtree(OUTPUT_PATH)


def test_traverse():
    # Test that we can fully traverse the directory and figure out the timestamp of each root node.
    generate_test_set(SAMPLE_DATA_PATH)

    result = analyze_directory(SAMPLE_DATA_PATH, IGNORE_PATHS, threshold_days=1)
    n_root_paths = sum([1 for _, x in result.items() if x.is_root])
    n_archive_files = sum(
        [1 for _, x in result.items() if x.should_archive and not x.is_dir]
    )

    # Print the result.
    display_paths(SAMPLE_DATA_PATH, result)
    assert n_root_paths == 13  # Expect 12 root paths.
    assert n_archive_files == 7  # Expect these many files to be archived.


def test_archive():
    generate_test_set(SAMPLE_DATA_PATH)

    result = analyze_directory(SAMPLE_DATA_PATH, IGNORE_PATHS, threshold_days=1)
    items = transfer_to_archive(result, ARCHIVE_PATH, ARCHIVE_FOLDER)
    assert len(items) == 7  # Expect these many files to be archived.


def test_upload():
    def fake_upload(path, bucket, key):
        print(f"Uploading [{path}, {bucket}, {key}]")
        time.sleep(0.2)

    mock_s3_client = Mock()
    mock_s3_client.create_bucket = Mock()
    mock_s3_client.upload_file = Mock(side_effect=fake_upload)

    bucket = "archive.bucket"
    generate_test_set(SAMPLE_DATA_PATH)

    result = analyze_directory(SAMPLE_DATA_PATH, IGNORE_PATHS, threshold_days=1)
    items = transfer_to_archive(result, ARCHIVE_PATH, ARCHIVE_FOLDER)
    upload(mock_s3_client, bucket, items)


def test_walk_files():
    # Once we transfer files to archives, we should be able to list them and get the keys.
    generate_test_set(SAMPLE_DATA_PATH)

    result = analyze_directory(SAMPLE_DATA_PATH, IGNORE_PATHS, threshold_days=1)
    original_items = transfer_to_archive(result, ARCHIVE_PATH, ARCHIVE_FOLDER)
    items = get_items_in_archive(ARCHIVE_PATH, ARCHIVE_FOLDER)

    # Test we get the same number of files.
    assert len(original_items) == len(items)

    # Test that the file keys are the same.
    original_map = {k: v for k, v in original_items}
    for item in items:
        assert item.key in original_map
        assert original_map[item.key] == item.path


def test_display_items():
    # Once we transfer files to archives, we should be able to list them and get the keys.
    generate_test_set(SAMPLE_DATA_PATH)

    result = analyze_directory(SAMPLE_DATA_PATH, IGNORE_PATHS, threshold_days=1)
    transfer_to_archive(result, ARCHIVE_PATH, ARCHIVE_FOLDER)
    items = get_items_in_archive(ARCHIVE_PATH, ARCHIVE_FOLDER)
    display_archive_items(items, estimate_cost=True)
