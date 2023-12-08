import os
import json
import logging
import typing as ty

from datetime import datetime
from dotenv import load_dotenv

from S3 import S3Pipeline


def parse_metadata(filename: str) -> ty.Union[int,ty.Dict[str,ty.Any]]:
    """
    Extract JSON data from metadata file 

    Args:
        `filename`: String filename of metadata txt file

    Returns:
        `content`: Decoded JSON object of experiment metadata
    
    """
    with open(filename,"r") as f:
        try:
            content = json.loads(f.read())
            print(f"Metadata Content is {content}")
            return content
      
        except json.JSONDecodeError as e:
            print(f"Error {e} File Content Not Properly Formatted")
            return None


def upload_to_bucket(path: str, filename: str, S3: S3Pipeline):
    data = parse_metadata(os.path.join(path,filename))
    if data:
        values = {"filename":filename, "content": data, 
                    "created":datetime.now(),"updated":datetime.now()
        }
        encoded_data = json.dumps(values, default=str)
        S3.write_data(encoded_data,filename)


def main(directory: str) -> None:
    """
    Runs our event handler in the background which will parse all new metadata files added to specified directory

    Args:
        directory: Name of directory where metadata files are to be found
    
    """
    load_dotenv('./.env')

    ACCESS_KEY= os.getenv('METADATA_ACCESS_KEY')
    SECRET_KEY= os.getenv('METADATA_SECRET_KEY')
    BUCKET_NAME = os.getenv('METADATA_BUCKET_NAME')

    S3 = S3Pipeline(ACCESS_KEY,SECRET_KEY, BUCKET_NAME)
    files = S3.get_filenames()

    if os.path.exists(directory):
        for file in os.listdir(directory):
            if file not in files:
                upload_to_bucket(directory, file, S3)
            else:
                print(f'File {file} Already Exists in Bucket') 
    else:
        print(f"Incorrect Metadata Path {directory} Specified")

if __name__ == "__main__":
    #replace this with actual path as shown below
    path = os.path.join("C:", os.sep, "FW", "WiWo", "metadata")
    path = os.path.join(os.path.dirname(__file__),'metadata')
    print(path)
    main(path)



