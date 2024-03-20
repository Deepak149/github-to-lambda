import json
import pandas as pd
import boto3

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn = 'arn:aws:sns:us-east-1:851725469799:s3-arrival-notification'

output_bucket_name = 'doordash-target-zone-deepak'

def lambda_handler(event, context):
    
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    s3_file_key = event["Records"][0]["s3"]["object"]["key"]
    print(bucket_name)
    print(s3_file_key)
    resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
    print(resp['Body'])
    data = pd.read_json(resp['Body'], lines=True)
    filtered_data = data[data['status'] == 'delivered']
    processed_json = filtered_data.to_json(orient='records', lines=True)
    s3_client.put_object(Bucket=output_bucket_name, Key=s3_file_key, Body=processed_json)
    print("processed_json = ", processed_json)
    print("events --> ", event)
    print("hello")
    