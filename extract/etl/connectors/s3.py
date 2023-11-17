import boto3
import logging
from io import BytesIO
import pandas as pd

class FantasyPLS3Client:

    def __init__(self):
        self.s3_client = boto3.client('s3')
    
    def deleteFilesFromBucketFolder(self, bucket_name: str, folder: str):
        self.bucket_name = bucket_name

        response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{folder}/")

        if response.get("Contents") is not None:
            files_in_folder = response["Contents"]
        
            files_to_delete = []

            for f in files_in_folder:
                files_to_delete.append({"Key": f["Key"]})
            
            self.s3_client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": files_to_delete})
    
    def dataframe_to_s3(self, input_datafame: pd.DataFrame, bucket_name: str, filepath: str, format: str):

        if format == 'parquet':
            out_buffer = BytesIO()
            input_datafame.to_parquet(out_buffer, index=False)

        self.s3_client.put_object(Bucket=bucket_name, Key=filepath, Body=out_buffer.getvalue())