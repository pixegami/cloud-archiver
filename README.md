# Cloud Archiver: Python CLI App

This is a Python CLI program that archives folders/files within a directory to an AWS S3 bucket. It scans your directory for folders/files rooted there which haven't been accessed for a while, and gives you the option to archive and delete them.

## Requirements

* Python 3.8
* AWS Account and CLI configured
* MacOS or Linux (it might also work on Windows, but I haven't tested it)

## Installation

You can install directly from `pip`.

```bash
pip install cloud-archiver
```

If you want to upload your files to the cloud, you will also need to have the AWS CLI installed and configured to your account. It will use the `default` profile to upload.

```bash
aws configure
```

## Usage

### Archiving Files

Run `cloud-archiver` command in the directory you want to work on. On the first run, it will create a  `.archive_config.json`, with some defaults.

```bash
cloud-archiver
```

This will take you through a guided prompt, displaying files/folders which are older than 60 days, (configurable) and asking what you want to do with them.

For a file/folder to be eligible for archive, it must be:

* Last accessed more than 60 days ago.
* Located in the root directory you are running the app from (so that folders won't be split up and partially archived).

You will be presented these choices in order. For each them them you must choose (`y/n`):

* **Archive**: This will move all eligible files to a hidden `.archive` folder in your directory.
* **Upload**: All files in the `.archive` folder (including ones that were moved there in a different session) will be uploaded to the S3 bucket and AWS account configured in `.archive_config.json`.
* **Delete**: Permanently delete all files in `.archive`.

### Generate Test Files

```bash
cloud-archiver --generate
```

This will create a handful of text files and folders in the current working directory for you to test the archiving functionality.

```bash
ls

# file_091ba_0d.txt  file_4cb35_0d.txt  file_6063f_180d.txt  file_9066c_180d.txt  file_c0083_180d.txt  test_dir_1
# file_2f1cc_0d.txt  file_53cfd_0d.txt  file_688e5_180d.txt  file_b54f3_180d.txt  file_e15ac_0d.txt    test_dir_2
```

## Configuration

Run this command to show the current configuration (or create it if it doesn't exist):

```
cloud-archiver --config
```

Configurations are stored in `.archive_config.json`, and are unique for each directory you decide to use the app in. You can edit the file directly to change the bucket or `days` considered fit for archiving.

```json
{
  "bucket": "cloud-archiver.34c55d712f2d.archivetest2",
  "days": 60
}
```