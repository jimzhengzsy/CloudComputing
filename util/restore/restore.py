import json
import boto3

from botocore.exceptions import ClientError

REGION = "us-east-1"
VAULT_NAME = "ucmpcs"
DYNAMODB_TABLE = 'songyuanzheng_annotations'

# https://docs.aws.amazon.com/lambda/latest/dg/welcome.html
# https://docs.aws.amazon.com/lambda/latest/dg/configuration-function-common.html
def lambda_handler(event, context):
    records = event['Records']
    for record in records:
        # https://docs.aws.amazon.com/lambda/latest/dg/with-sns.html
        # Load message from SNS
        data = json.loads(record['Sns']['Message'])
        if not data['Completed'] or not data['StatusCode'] == 'Succeeded':
            continue
        job_id, archive_id = data['JobId'], data['ArchiveId']
        # get data
        glacier = boto3.client('glacier', region_name=REGION)
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html?highlight=glacier#Glacier.Client.get_job_output
        try:
            resp = glacier.get_job_output(
                vaultName=VAULT_NAME,
                jobId=job_id
            )
        except ClientError as e:
            print(f'Fail to download data {e}')
            continue
        # upload data to s3
        annotation_job_id = data['JobDescription']
        
        dynamodb = boto3.client('dynamodb', region_name=REGION)
        try:
            resp = dynamodb.get_item(
                TableName=DYNAMODB_TABLE,
                Key={'job_id': {'S': annotation_job_id}},
                ProjectionExpression='s3_key_result_file,s3_results_bucket'
            )
        except ClientError as e:
            print(f'Fail to get item from dynamodb: {e}')
            continue
        if 'Item' not in resp:
            print(f'No such job {annotation_job_id}')
            continue
        item = resp['Item']
        
        bucket = item['s3_key_result_file']['S'] 
        key = item['s3_results_bucket']['S']
        
        s3 = boto3.client('s3', region_name=REGION)
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_fileobj.html#
        # Example of upload file object
        try:
            s3.upload_fileobj(resp['body'], bucket, key)
        except ClientError as e:
            print(f'Unable to upload file to s3 {e}')
            continue
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html?highlight=glacier#Glacier.Client.delete_archive
        # Example of delete_archive
        glacier = boto3.client('glacier', region_name=REGION)
        try: 
            glacier.delete_archive(
                vaultName=VAULT_NAME,
                archiveId=archive_id
            )
        except ClientError as e:
            print(f'Fail to delete file in Glacier: {e}')
            continue
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
        # Update dynamodb and remove archive_id
        dynamodb = boto3.client('dynamodb', region_name=REGION)
        try:
            dynamodb.update_item(
                TableName=DYNAMODB_TABLE,
                Key={'job_id': {'S': annotation_job_id}},
                UpdateExpression='REMOVE results_file_archive_id'
            )
        except ClientError as e:
            print(f'Fail to update dynamodb: {e}')
            continue