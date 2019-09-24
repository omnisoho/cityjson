from pymongo import MongoClient
import json

def main():

    # Get a MongoClient object
    # connectionObject = MongoClient('mongodb://localhost:27017/')
    connection_Object = MongoClient('mongodb://ec2-13-250-113-174.ap-southeast-1.compute.amazonaws.com:27017/')
    print('connected')
    
    database_Object = connection_Object.hdbcityjson
    print('db referenced')

    collection_object = database_Object.cityjson
    print('collection referenced')

    # # insert a simple json document into the test collection
    # collection_object.insert({"red":123, "green":223, "blue":23})
    # print('inserted')

    ret_list = []

    # Using find() query all the documents from the collection
    id_list = ["way/172927954", "way/325836609", "way/172367110"]
    query = collection_object.find({'lod':1, 'part_id': {'$in': id_list}}, {'blob':1, 'meta':1, '_id':0})
    for document in query:
        # print each document
        ret_list.append(document['blob'])
        ret_list.append(document['meta'])
        # print(document)

    # print(json.dumps(ret_list))
    print(query.count())

if __name__ == '__main__':
    main()
