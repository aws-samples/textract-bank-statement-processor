"""Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 
SPDX-License-Identifier: MIT-0
"""
import boto3
import os
import random
import json
import csv
from re import sub

client = boto3.client('stepfunctions')
s3_client = boto3.client('s3')

def convert_str_to_float(value):
    try:
        return float(sub(r'[^\d.]', '', value))
    except Exception as e:
        print(e)
        return 0.0

def handle(event, context):
    print(event)
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    response = s3_client.head_object(Bucket=bucket_name, Key=key)
    print('Response: {}'.format(response))
    statement_type = response['Metadata']['statement_type']
    if statement_type == 'ing':
        handle_csv_statement(bucket_name, key)
    elif statement_type == 'cba_cc' or statement_type == 'cba_bank':
        stateMachineARN = os.environ['statemachine_arn']
        response = client.start_execution(
            stateMachineArn=stateMachineARN,
            name='test-sf'+str(random.randint(10, 100000)),
            input=json.dumps(event['Records'][0])
        )

def handle_csv_statement(bucket_name, key):
    print('Handle CSV statement')
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    contents = response['Body'].read().decode('utf-8').splitlines()

    result = []
    csvReader = csv.DictReader(contents)
    for row in csvReader:
       result.append(row)

    for transaction in result:
        transaction['Debit'] = convert_str_to_float(transaction['Debit'].lstrip('-'))
        transaction['Credit'] = convert_str_to_float(transaction['Credit'])

    output_bucket = os.environ['OUTPUT_BUCKET']
    output_prefix = os.environ['OUTPUT_PREFIX']

    output_object_base = os.path.join(output_prefix, os.path.abspath(key))

    output_object = f"{output_object_base}.json"
    s3_client.put_object(
        Bucket=output_bucket,
        Key=output_object,
        Body=(bytes(json.dumps(result).encode('UTF-8'))),
        ServerSideEncryption='AES256',
        ContentType='application/json',
    )
    print(f"File saved to: s3://{output_bucket}/{output_object}")

