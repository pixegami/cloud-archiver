import os
import uuid
from datetime import timedelta, datetime


def generate_test_set(path: str):
    # Generate some files in the base directory.
    generate_test_files(5, path, days_old=0)
    generate_test_files(5, path, days_old=180)

    # Generate a directory with some files.
    # This should NOT be archived.
    test_dir_1 = generate_directory(path, "test_dir_1")
    generate_test_files(4, test_dir_1, days_old=0)
    generate_test_files(1, test_dir_1, days_old=180)  # Even though these are old, the folder was touched recently.

    # Generate a directory. This one has no files, but has a nested dir with some old files.
    # These should be archived.
    test_dir_2 = generate_directory(path, "test_dir_2")
    nested_dir_1 = generate_directory(test_dir_2, "nested_dir_1")
    generate_test_files(2, nested_dir_1, days_old=180)

    # Hidden directory --- should ignore?
    test_dir_3 = generate_directory(path, ".archive")
    generate_test_files(2, test_dir_3, days_old=6)


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
