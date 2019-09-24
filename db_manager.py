
import boto3
from datetime import datetime
from abc import ABC, abstractmethod
from credentials_config import AWS_REGION, ACCESS_ID, AWS_SECRET_KEY
from pymongo import MongoClient


class DatabaseManager(ABC):
    
    @abstractmethod
    def upload_items(self):
        pass

    @abstractmethod
    def upload_item(self):
        pass

class DynamodbManager(DatabaseManager):

    def __init__(self, tableName):
        # db connection
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, 
        aws_access_key_id=ACCESS_ID, 
        aws_secret_access_key= AWS_SECRET_KEY)
        self._table = dynamodb.Table(tableName)

    def upload_items(self, dataDict, lod):

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
        # put_item - Creates a new item, or replaces an old item with a new item. 
        #  If an item that has the same primary key as the new item already exists 
        #  in the specified table, the new item completely replaces the existing item.
        self._table.put_item(
            Item=data_dict
        )        


class MongodbManager(DatabaseManager):

    def __init__(self, table_name):

        self.table_name = table_name

        # db connection
        self.connection_Object = MongoClient('mongodb://ec2-13-250-113-174.ap-southeast-1.compute.amazonaws.com:27017/') 
        print('connected')
        
        self.database_Object = self.connection_Object.hdbcityjson
        print('db referenced')

        self.collection_object = getattr(self.database_Object, self.table_name)
        print('collection referenced')

    def upload_items(self, dataDict, lod):

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
        # self.collection_object.insert(data_dict) 
        filter_key = {'part_id': data_dict['part_id'], 'lod': data_dict['lod']}
        update_actions = {'blob': data_dict['blob'], 'meta': data_dict['meta']}

        # upsert (optional): If True, perform an insert if no documents match the filter_key.
        self.collection_object.update_one( filter_key,
                            {'$set': update_actions}, 
                            upsert=True)