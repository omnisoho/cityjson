from city_json_data import CityJsonData
from db_manager import DynamodbManager

def main():

    table_name = 'HdbCityJson'
    obj_type_str = 'city_json'
    default_lod = 1
    local_filename = './hdb.json'
    
    city_json_processer = CityJsonData(obj_type_str, default_lod, local_filename)
    city_json_processer.preprocess()

    data_dict = city_json_processer.get_data_dictionary()
    print (data_dict)

    data_manager = DynamodbManager(table_name)
    data_manager.upload_list(data_dict, city_json_processer.lod())

if __name__ == '__main__':
    main()


