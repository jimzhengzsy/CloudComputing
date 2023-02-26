# archive_script.py
#
# Archive free user data
#
# Copyright (C) 2011-2021 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import boto3
import time
import os
import sys
import json
import psycopg2
from botocore.exceptions import ClientError
import logging
import jmespath

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('archive_script_config.ini')

region = config['aws']['AwsRegionName']
valt_name = config['glacier']['VAULT_NAME']
wait_time = config['sqs']['WAIT_TIME_SECONDS']
sqs_archive_url = config['sqs']['SQS_ARCHIVE_URL']

'''Capstone - Exercise 7
Archive free user results files
'''
def handle_archive_queue(sqs=None):
  if sqs is None:
    print('Error: sqs handler is empty')
    return

  # Read a message from the queue
  try:
    resp = sqs.receive_message(
      QueueUrl=sqs_archive_url,
      WaitTimeSeconds=20
    )
  except ClientError as e:
    print(f'Fail to receive messages from sqs: {e}')
    return

  resp = jmespath.search('Messages[*].{handle: ReceiptHandle, body: Body}', resp)
  if resp is None:
    return
  for message in resp:
    try:
      data = json.loads(message['body'])
      data = json.loads(data['Message'])
      data = json.loads(data['notification']['message'])
    except json.decoder.JSONDecodeError as e:
      print(f'Fail to decode message: {message["body"]} {e}')
      continue
    # process message

    print(f"The data is {data}")
    user_id = data['user_id']
    if not is_free_user(user_id):
      print('It is a premium user')
    else:
      print('It is a free user')
      archive(data['bucket'], data['filename'], data['job_id'])
    # delete message
    try:
      response = sqs.delete_message(
          QueueUrl=sqs_archive_url,
          ReceiptHandle=message['handle']
      )
    except ClientError as e:
      print(f'Fail to delete the message from sqs: {e}')  

def archive(bucket, filename, job_id):
  # download file from s3
  try:
    s3 = boto3.resource('s3', region_name=region)
    obj = s3.Object(bucket, filename)
    bytes = obj.get()['Body'].read()
  except ClientError as e:
    print(f'Fail to download the original file {e}')
    return
  # upload the file to glacier
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.upload_archive
  try:
    glacier = boto3.client('glacier', region_name=region)
    resp = glacier.upload_archive(
        vaultName=valt_name,
        body=bytes
    )
  except ClientError as e:
    print(f'Unable to upload file to glacier {e}')
    return
  # delete file in s3:
  try:
    s3 = boto3.client('s3', region_name=region)
    s3.delete_object(Bucket=bucket, Key=filename)
  except ClientError as e:
    print(f'Unable to delete file in s3 {e}')
  # update dynamodb
  try:
    archive_id = resp['archiveId']
    dynamodb = boto3.client('dynamodb', region_name=region)
    dynamodb.update_item(TableName=config['dynamodb']['DynamoDBAnnotationsTable'],
                        Key={'job_id': {'S': job_id}},
                        UpdateExpression='SET results_file_archive_id = :id',
                        ExpressionAttributeValues={
                            ':id': {'S': archive_id}
                        })
  except ClientError as e:
    print(f'Fail to update dynamodb {e}')
  print(f'successfully archive file {filename}')


def is_free_user(user_id):
    try:
        profile = helpers.get_user_profile(id=user_id)
    except Exception as e:
        print(f'Fail to get user profile: {e}')
        return False
    return profile['role'] == 'free_user'

def main():
    # Get handles to resources; and create resources if they don't exist
    sqs = boto3.client('sqs', region_name=region)
    # Poll queue for new results and process them
    while True:
      handle_archive_queue(sqs=sqs)

if __name__ == '__main__':  
    main()

### EOF