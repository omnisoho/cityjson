from credentials_config import AWS_REGION, ACCESS_ID, AWS_SECRET_KEY
import boto3

class AccessManager(object):

    def __init__(self):
        self._dynamodb_instance = boto3.resource('dynamodb', region_name=AWS_REGION, 
        aws_access_key_id=ACCESS_ID, 
        aws_secret_access_key= AWS_SECRET_KEY)

        self._s3_client_instance = boto3.client('s3', region_name=AWS_REGION, 
        aws_access_key_id=ACCESS_ID, 
        aws_secret_access_key= AWS_SECRET_KEY)
     

    def dynamodb_instance(self):
        return self._dynamodb_instance

    def s3_client_instance(self):
        return self._s3_client_instance
        