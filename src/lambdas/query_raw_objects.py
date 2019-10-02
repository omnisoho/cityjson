import msgpack
import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import base64 
from botocore.config import Config
from time import sleep

def lambda_handler(event, context):

    MAX_RETRY_ATTEMPT = 6
    BATCH_ITEM_MAX_LIMIT = 100     # batch_get_item limited max 100 items or 16MB per call
    THROTTLE_DURATION_IN_SEC = 0.2     # sleep duration to throttle batch_get_item requests to dynamodb

    print('lambda started')
    table_name = 'RawObjects'
    
    config = Config(retries={'max_attempts': MAX_RETRY_ATTEMPT})    
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-1', config=config)
    # table = dynamodb.Table()
    
    s3 = boto3.client('s3', region_name='ap-southeast-1')
    
    
    print(event['body-json'])
    # input_str = base64.b64decode(event['body-json'])
    # print('base64 decoded: ')
    # print(input_str)
    # input_str = event['body-json']
    # json_obj = json.loads(input_str)
    # print('json obj:')
    json_obj = event['body-json']
    print(json_obj)
    lod = json_obj['lod']
    id_list = json_obj['id_list']
    raw_format = True if 'raw_format' in json_obj and json_obj['raw_format'] == True else False
    
    # print(id_list)

    # id_list  = ["way/116692213","way/172367110","way/42064045"]
    # formatted_id_list = [{'partition_id':x,'lod':1} for x in id_list]
    formatted_id_list = []
    for x in id_list:
        formatted_id_list.append({'partition_id': x, 'lod':1})
        
    test_id_list = [{'partition_id': 'way/116692213', 'lod':1}, { 'partition_id': 'way/172367110', 'lod':1}]
    # print(formatted_id_list)


    n = BATCH_ITEM_MAX_LIMIT    # use list comprehension to split id_list into batches
    splitted_id_list = [formatted_id_list[i * n:(i + 1) * n] for i in range((len(formatted_id_list) + n - 1) // n )]  
    obj_list = []
    
    print('start querying')
    try:
        for i in range(len(splitted_id_list)):
                sleep(0.08)
                response = dynamodb.batch_get_item(
                    RequestItems={
                        table_name:{
                            'Keys': splitted_id_list[i],
                        },
                    },
                    ReturnConsumedCapacity='TOTAL'
                )
        
                obj_list.extend(response['Responses'][table_name])
    except Exception as e:
            print(e)

    # print(obj_list)
    obj_count = len(obj_list)
    print('obj count:')
    print(obj_count)

    packing_list = []
    packing_list.append(obj_count)
    

    for item in obj_list:
        
        if raw_format:
            s3file = s3.get_object(Bucket=item['raw_bucket'], Key=item['raw_key'])
            content = s3file['Body'].read()
            meta = item['meta']
            # print(item['raw_bucket'])
            # print(item['raw_key'])
            # print(meta)
            packing_list.append(content)
            packing_list.append(meta)
        else:
            s3file = s3.get_object(Bucket=item['mesh_bucket'], Key=item['mesh_key'])
            content = s3file['Body'].read()
            meta = item['meta']
            # print(item['mesh_bucket'])
            # print(item['mesh_key'])
            # print(meta)
            packing_list.append(content)
            packing_list.append(meta)
        

    # packing_list = []
    # packing_list.append(obj_count)
    
    # print('prepacked')
    # for item in full_object_list:
    #     packing_list.append(item['blob'].value)
    #     packing_list.append(item['meta'])
    
    print('pack completed')
    bdata = msgpack.packb(packing_list, use_bin_type=True)
    # print(bdata)
    
    return bdata
