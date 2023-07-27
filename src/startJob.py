"""Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 
SPDX-License-Identifier: MIT-0
"""
import json
import time
import boto3
import sys

textract_client = boto3.client('textract')
s3_client = boto3.client('s3')

def startJob(bucket_name, object_name):
    response = textract_client.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': object_name,
            }
        },
        FeatureTypes=['TABLES'],
    )

    return response["JobId"]

def handle(event, context):
    if event['eventSource'] != 'aws:s3':
        print("ERROR: Unexpected event type")
        print(json.dumps(event))
        raise ValueError("ERROR: Unexpected event type")

    bucket_name = event['s3']['bucket']['name']
    key = event['s3']['object']['key']

    response = s3_client.head_object(Bucket=bucket_name, Key=key)
    
    print('Response: {}'.format(response))

    print(f"StartJob: s3://{bucket_name}/{key}")
    statement_type = response['Metadata']['statement_type']

    job_id = startJob(bucket_name, key)
    print(f"JobId: {job_id}")

    return {
        "bucket_name": bucket_name,
        "object_name": key,
        "job_id": job_id,
        "job_start_timestamp": time.time(),
        "statement_type": statement_type,
    }

if __name__ == "__main__":
    import sys
    with open(sys.argv[1], "rt") as f:
        event = json.load(f)
    ret = handler(event, {})
    print(json.dumps(ret, indent=2))