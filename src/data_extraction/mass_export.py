from .city_json_data import CityJsonData
from src.db_manager import DynamodbManager, MongodbManager
from src.constants import FILETYPE_MAPPING

def main():

    table_name = 'RawObjects'
    test_table_name = 'TestTable'
    obj_type_str = FILETYPE_MAPPING['CITYJSON']
    default_lod = 1
    local_filename = './hdb.json'
    is_test_run = True
    
    # s3_bucket = 'prj-ss-raw-data'
    s3_bucket = 'gisgeometry.raw.data'
    mesh_s3_file_path = 'hdb_cityjson/exported_objs/'
    rawdata_s3_file_path = 'hdb_cityjson/raw_data/'

    city_json_processer = CityJsonData(obj_type_str, default_lod, local_filename)
    city_json_processer.preprocess(is_test_run)

    data_dict = city_json_processer.get_data_dictionary()
    object_type = city_json_processer.get_obj_type()
    # print (data_dict)

    data_manager = DynamodbManager(table_name, s3_bucket, rawdata_s3_file_path, mesh_s3_file_path)
    # data_manager = MongodbManager(test_table_name)
    print('data_manager instantiated')
    data_manager.upload_items(data_dict, object_type, city_json_processer.lod()) 
    print('data_manager uploaded')

if __name__ == '__main__':
    main()


