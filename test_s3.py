import boto3
from credentials_config import AWS_REGION, ACCESS_ID, AWS_SECRET_KEY

def main():

    bucket = 'prj-ss-ingestion-data'
    file_key = 'hdb-city-json/hdb.json'
    test_file_key = 'test.txt'

    s3 = boto3.client('s3', region_name=AWS_REGION, 
    aws_access_key_id=ACCESS_ID, 
    aws_secret_access_key= AWS_SECRET_KEY)

    s3file = s3.get_object(Bucket=bucket, Key=test_file_key)
    content = s3file['Body'].read()
    print(content)

if __name__ == '__main__':
    main()