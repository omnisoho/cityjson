from city_json_data import CityJsonData
from db_manager import DynamodbManager, MongodbManager

def main():

    table_name = 'HdbCityJson'
    test_table_name = 'TestTable'
    obj_type_str = 'city_json'
    default_lod = 1
    local_filename = './hdb.json'
    is_test_run = True
    
    city_json_processer = CityJsonData(obj_type_str, default_lod, local_filename)
    city_json_processer.preprocess(is_test_run)

    data_dict = city_json_processer.get_data_dictionary()
    # print (data_dict)

    data_manager = DynamodbManager(test_table_name)
    # data_manager = MongodbManager(test_table_name)
    print('data_manager instantiated')
    data_manager.upload_items(data_dict, city_json_processer.lod()) 
    print('data_manager uploaded')

if __name__ == '__main__':
    main()


