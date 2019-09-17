
import boto3
from datetime import datetime

class DynamodbManager(object):

    # credentials
    ACCESS_ID = 'AKIA3WGSJSFJRWPHTQVX'
    AWS_SECRET_KEY = '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'

    def __init__(self, tableName):
        # db connection
        dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-1', 
        aws_access_key_id=DynamodbManager.ACCESS_ID, 
        aws_secret_access_key= DynamodbManager.AWS_SECRET_KEY)
        self._table = dynamodb.Table(tableName)

    def upload_list(self, dataDict, lod):

        for key, value in dataDict.items():
            data_obj = value
            print(key)
            print(data_obj.key_id())
            print(data_obj.meta_data())
            print(data_obj.compressed_data())
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