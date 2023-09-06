# File Watcher
This repo contains all files necessary for uploading Fermworks experiment metadata to AWS cloud in real time

# Instructions:

To run the file watcher use the script start.ps1 by running `./start.ps1`, this will create a virtual environment and install all dependencies

# Contents

## `file_watcher.py`

Contains main function that creates a watchdog event and listener that will listen for updates on a specified folder (folder that contains FW metadata files). 
Once a file has been added it will be stored to a local SQLite table and uploaded to an AWS S3 bucket if no duplicate entry exists in our local db

## `connection.py`

Contains class that handles our connection to our SQLite db. Handles engine creation, connection instantialization and CRUD operations

## `models.py`

Contains all model classes defined using SQLAlchemy's ORM tools. 

## `S3.py`

Contains wrapper class that handles connection, authentication, and reading and writing data to our AWS S3 bucket
