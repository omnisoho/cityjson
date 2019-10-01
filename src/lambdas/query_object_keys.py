import json
import boto3

def lambda_handler(event, context):
    
    print('GetAllKeys lambda started')
    
    # TODO implement
    
    table_name = 'RawObjects'
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-1')
    table = dynamodb.Table(table_name)

    # query - batch get
    response = dynamodb.Table(table_name).scan()
    
    print('GetAllKeys scanned')
    
    pe = "partition_id"

    response = table.scan(
        ProjectionExpression=pe,
        )

    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ProjectionExpression=pe,
            ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    processed_data = [x['partition_id'] for x in data]

    print(len(processed_data))    
    print('GetAllKeys lambda ended')
    
    return {
        'statusCode': 200,
        'body': json.dumps(processed_data)
    }
