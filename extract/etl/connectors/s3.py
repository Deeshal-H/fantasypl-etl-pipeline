import boto3
import logging

class FantasyPLS3Client:

    def __init__(self):
        pass
    
    def deleteFilesFromBucketFolder(self, bucket_name: str, folder: str):
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name

        response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{folder}/")

        if response.get("Contents") is not None:
            files_in_folder = response["Contents"]
        
            files_to_delete = []

            for f in files_in_folder:
                files_to_delete.append({"Key": f["Key"]})
            
            self.s3_client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": files_to_delete})