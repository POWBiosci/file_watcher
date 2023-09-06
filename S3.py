"""
This module contains pipelines for the SOFE benchmark.
"""
from __future__ import annotations

import json
import os
import typing as ty
import boto3
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, EndpointConnectionError, ClientError

class S3Pipeline:
    """
    Base Class for our Pipeline Object that allows for IRL retrieval of reactor 
    data from AWS S3 bucket.

    example calls:
    new_object = Pipeline(ACCESS_KEY, SCRET_KEY, BUCKET_NAME)
    all_data = Pipeline.get_data()
    specific_query = Pipeline.get_value('F2', 'TEMP)
    """
    def __init__(self, access_key: str, secret_key: str, bucket_name: str) -> None:
        """
        Initialize API object. 

        Args:
            access_key (str): AWS access key for accessing our S3 bucket.
            secret_key (str): AWS secret key for confirming our correct user 
                permissions.
            bucket_name (str): Name of S3 bucket that holds our data.

        Returns:
                None

        Raises:
            NoCredentialsError: If the credentials provided are invalid.
            EndpointConnectionError: If there is an error connecting to the endpoint.
        """
        self.AWS_SECRET_KEY = secret_key
        self.AWS_ACCESS_KEY = access_key
        self.BUCKET_NAME = bucket_name

        try:
            self.session = boto3.Session(
                aws_access_key_id=self.AWS_ACCESS_KEY,
                aws_secret_access_key=self.AWS_SECRET_KEY,
            )
        except NoCredentialsError:
            print("Invalid Credentials Provided")
        except EndpointConnectionError:
            print("Connection Error")

        self.s3 = self.session.client("s3")

    def get_last_payload(self) -> ty.Optional[str]:
        """
        Obtains the filename of the most recent timestamp data pushed 
        to S3 bucket.

        Returns:
            str: String filename of object to query.

        Raises:
            ClientError: If there is an error with the client.
        """
        try:
            response = self.s3.list_objects(
                Bucket=self.BUCKET_NAME)["Contents"]

            # Sort files by most recent upload:
            sorted_data = sorted(response, key=lambda obj: obj["LastModified"])

            last_payload = sorted_data[-1]['Key']
            return last_payload

        except ClientError as e:
            print(f"Error encountered: {e.response['Error']['Code']}")

    def get_data(self, file_name:str) -> ty.Optional[ty.List[ty.Dict[str, ty.Any]]]:
        """
        Returns all of the data from the most recent S3 payload.

        -Args:
            file_name: Name of file to extract data from

        -Returns:
            decoded_data: List of dictionaries containing all of the data from the file.
        """
        try:
            response = self.s3.get_object(Bucket=self.BUCKET_NAME, Key=file_name)["Body"]
            file_content = response.read().decode("utf-8")

            # Convert our decoded string to json:
            decoded_data = json.loads(file_content)

            return decoded_data

        except ClientError as err:
            print(f"Error encountered: {err.response['Error']['Code']}")
    
    def write_data(self, data: ty.Dict[str, ty.Any], file_name: str) -> None:
        """"
        Method for writing data to our S3 bucket from our simulation results

        Args:
            data (ty.Dict[str, ty.Any]): JSON data to be uploaded into our file
            file_identifier: File identifier of out file.

        Returns:
            None

        Raises:
            EndpointConnectionError: If there is an error connecting to the
                endpoint.
        """
        try:
            self.s3.put_object(Body=data, Bucket=self.BUCKET_NAME, Key=file_name)
            print(f"Writing to {self.BUCKET_NAME} completed with filename: {file_name}")

        except EndpointConnectionError as e:
            print(f"Endpoint connection error {e}")


    def get_filenames(self) -> ty.List[str]:
        """
        Lists all files in S3 bucket

        -Returns:
            files: List of filenames of all files in bucket
        """

        files=[]
        try:
            all_files = self.s3.list_objects(Bucket=self.BUCKET_NAME)
            paginator = self.s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.BUCKET_NAME)
            for _ in pages:
                for file in all_files['Contents']:
                    files.append(file['Key'])
        
        except ClientError as e:
            print(f"Error encountered: {e.response['Error']['Code']}")
        
        return files


