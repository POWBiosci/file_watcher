# File Watcher
This repo contains all files necessary for uploading Fermworks experiment metadata to AWS cloud in real time

# Instructions:

To run the file watcher use the script start.ps1 by running `./start.ps1`, this will create a virtual environment and install all dependencies

# Contents

## `file_watcher.py`

Checks file contents of given directory and adds all files that do not exist inside the S3 bucket to the bucket.

## `S3.py`

Contains wrapper class that handles connection, authentication, and reading and writing data to our AWS S3 bucket
