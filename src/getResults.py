"""Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 
SPDX-License-Identifier: MIT-0
"""
import os
import csv
import io
import json
import time
import boto3
import csv
from collections import defaultdict

from src.cbaCCRowProcessor import CBACCRowProcessor
from src.cbaSavingsRowProcessor import CBASavingsRowProcessor

cba_cc_row_processor = CBACCRowProcessor()
cba_savings_row_processor = CBASavingsRowProcessor()

s3_client = boto3.client('s3')
textract_client = boto3.client('textract')

def generate_table_csv(table_result, blocks_map, table_index):
    rows = get_rows_columns_map(table_result, blocks_map)
    csv = ''
    for row_index, cols in rows.items():
        for col_index, text in cols.items():
            csv += '{}'.format(text).strip() + "|"
        csv += '\n'
    return csv

def get_rows_columns_map(table_result, blocks_map):
    rows = {}
    for relationship in table_result['Relationships']:
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = blocks_map[child_id]
                if cell['BlockType'] == 'CELL':
                    row_index = cell['RowIndex']
                    col_index = cell['ColumnIndex']
                    if row_index not in rows:
                        # create new row
                        rows[row_index] = {}
                    # get the text value
                    rows[row_index][col_index] = get_text(cell, blocks_map)
    return rows

def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] =='SELECTED':
                            text +=  'X '    
    return text

def getJobResults(job_id, next_token = None):
    kwargs = {}
    if next_token:
        kwargs['NextToken'] = next_token

    response = textract_client.get_document_analysis(JobId=job_id, **kwargs)

    return response



def handle(event, context):
    blocks_map = {}
    table_blocks = []
    statement_name = event['object_name']
    job_id = event['job_id']
    statement_type = event['statement_type']

    results = getJobResults(job_id)
    event['job_status'] = results['JobStatus']
    event['job_update_timestamp'] = time.time()

    if event['job_status'] != "SUCCEEDED":
        if event['job_status'] != "IN_PROGRESS":
            event['results'] = results
        return event

    # Job succeeded - retrieve the results
    input_bucket = event['bucket_name']
    input_object = event['object_name']

    output_bucket = os.getenv('OUTPUT_BUCKET', input_bucket)
    output_prefix = os.environ['OUTPUT_PREFIX']
    output_object_base = output_prefix + input_object

    event['output_bucket'] = output_bucket
    blocks = []

    while True:
        if 'Blocks' in results:
            blocks.extend(results['Blocks'])
            for block in results['Blocks']:
                blocks_map[block['Id']] = block
                if block['BlockType'] == "TABLE":
                    table_blocks.append(block)

        if 'NextToken' not in results:
            break

        print(f"NextToken: {results['NextToken']}")
        results = getJobResults(job_id, next_token=results['NextToken'])

    rows = []
    for index, table in enumerate(table_blocks):
        table_csv = generate_table_csv(table, blocks_map, index +1)

        print(table_csv)

        if (table_csv.startswith('Date')):
            data = io.StringIO(table_csv.strip())
            
            for row in csv.DictReader(data, delimiter="|", quoting=csv.QUOTE_NONE):
                try:
                    #Use statement type from S3 metadata to determine which row processor to use
                    if statement_type == 'cba_cc':
                        rows.append(cba_cc_row_processor.process_row(row, statement_type, statement_name))
                    elif statement_type == 'cba_bank':
                        rows.append(cba_savings_row_processor.process_row(row, statement_type, statement_name))
                except Exception as e:
                    print(e)

    output_object = f"{output_object_base}.json"
    s3_client.put_object(
        Bucket=output_bucket,
        Key=output_object,
        Body=(bytes(json.dumps(rows).encode('UTF-8'))),
        ServerSideEncryption='AES256',
        ContentType='application/json',
    )
    print(f"Blocks file saved to: s3://{output_bucket}/{output_object}")
    event['blocks'] = output_object

    return event