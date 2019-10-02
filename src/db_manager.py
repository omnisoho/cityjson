from datetime import datetime
from abc import ABC, abstractmethod
from pymongo import MongoClient
from src.aws_access_manager import AccessManager


class DatabaseManager(ABC):

    @abstractmethod
    def upload_items(self):
        pass

    @abstractmethod
    def upload_item(self):
        pass

class DynamodbManager(DatabaseManager):

    PARTITION_ID = 'partition_id'
    LOD = 'lod'
    OBJECT_TYPE = 'file_type'
    MESH_BUCKET = 'mesh_bucket'
    MESH_KEY = 'mesh_key'
    RAW_BUCKET = 'raw_bucket'
    RAW_KEY = 'raw_key'
    META = 'meta'
    LAST_UPDATED_DATETIME = 'last_updated_datetime'

    def __init__(self, table_name, bucket, raw_filepath, mesh_filepath=None):
        # db connection
        # dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, 
        # aws_access_key_id=ACCESS_ID, 
        # aws_secret_access_key= AWS_SECRET_KEY)
        # self._table = dynamodb.Table(table_name)
        access_manager = AccessManager()
        dynamodb = access_manager.dynamodb_instance()
        self._table = dynamodb.Table(table_name)

        #s3 connection
        # self._s3 = boto3.client('s3', region_name=AWS_REGION, 
        # aws_access_key_id=ACCESS_ID, 
        # aws_secret_access_key= AWS_SECRET_KEY)

        self._s3 = access_manager.s3_client_instance()
        self._bucket = bucket
        self._raw_filepath = raw_filepath
        self._mesh_filepath = mesh_filepath

    def upload_items(self, dataDict, object_type, lod):

        for key, value in dataDict.items():
            data_obj = value
            # print(key)
            # print(data_obj.key_id())
            # print(data_obj.meta_data())
            # print(data_obj.compressed_data())

            part_id = data_obj.formatted_key_id()
            mesh = data_obj.compressed_mesh()
            mesh_key_path = self._mesh_filepath + part_id
            self._s3.put_object(
                Body=mesh, Bucket=self._bucket, Key=mesh_key_path, 
                # ACL='public-read'
            )

            raw_data = data_obj.compressed_data()
            raw_key_path = self._raw_filepath + part_id
            self._s3.put_object(
                Body=raw_data, Bucket=self._bucket, Key=raw_key_path, 
                # ACL='public-read'
            )                        

            uploadDataDict = {
                DynamodbManager.PARTITION_ID: part_id,
                DynamodbManager.LOD: lod,
                DynamodbManager.OBJECT_TYPE: object_type,
                DynamodbManager.MESH_BUCKET: self._bucket,
                DynamodbManager.MESH_KEY: mesh_key_path,
                DynamodbManager.META: data_obj.meta_data(),
                DynamodbManager.RAW_BUCKET: self._bucket,
                DynamodbManager.RAW_KEY: raw_key_path
            }
            self.upload_item(uploadDataDict)

    def upload_item(self, data_dict):
        
        data_dict[DynamodbManager.LAST_UPDATED_DATETIME] = str(datetime.now())
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