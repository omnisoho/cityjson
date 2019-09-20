
import boto3
from datetime import datetime
from credentials_config import AWS_REGION, ACCESS_ID, AWS_SECRET_KEY

class DynamodbManager(object):

    def __init__(self, tableName):
        # db connection
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, 
        aws_access_key_id=ACCESS_ID, 
        aws_secret_access_key= AWS_SECRET_KEY)
        self._table = dynamodb.Table(tableName)

    def upload_list(self, dataDict, lod):

        for key, value in dataDict.items():
            data_obj = value
            # print(key)
            # print(data_obj.key_id())
            # print(data_obj.meta_data())
            # print(data_obj.compressed_data())
            uploadDataDict = {
                'part_id': str(data_obj.key_id()),
                'blob': data_obj.compressed_data(),
                'meta': data_obj.meta_data(),
                'lod': lod,
            }
            self.upload_item(uploadDataDict)

    def upload_item(self, data_dict):
        
        data_dict['last_modified_date'] = str(datetime.now())
        self._table.put_item(
            Item=data_dict
        )        