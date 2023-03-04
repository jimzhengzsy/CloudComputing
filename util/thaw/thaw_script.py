# thaw_script.py
#
# Thaws upgraded (premium) user data
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
import jmespath
from botocore.exceptions import ClientError

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('thaw_script_config.ini')

region = config['aws']['AwsRegionName']
valt_name = config['glacier']['VaultName']
sqs_thaw_url = config['sqs']['SQSThawUrl']
wait_time = config['sqs']['WaitTimeSeconds']
job_restore_topic = config['sns']['JobRestoreTopic']

glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])

'''Capstone - Exercise 9
Initiate thawing of archived objects from Glacier
'''

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html?highlight=glacier#Glacier.Client.initiate_job
# https://docs.aws.amazon.com/amazonglacier/latest/dev/downloading-an-archive-two-steps.html

def handle_thaw_queue(sqs=None):
  if sqs is None:
    print('The SQS is None')
    return
  # Read a message from the queue
  try:
    resp = sqs.receive_message(
        QueueUrl=sqs_thaw_url,
        WaitTimeSeconds=int(wait_time)
    )
  except ClientError as e:
    print(f'Fail to retrieve message from sqs:{e}')
  # Process message
  resp = jmespath.search('Messages[*].{handle: ReceiptHandle, body: Body}', resp)
  if resp is None:
    return
  for message in resp:
    try:
      data = json.loads(message['body'])
      data = json.loads(data['Message'])
    except json.decoder.JSONDecodeError as e:
      print(f'Fail to decode message: {message["body"]} {e}')
      continue

    if 'archive_id' in data and 'job_id' in data:
      archive_id = data['archive_id']
      job_id = data['job_id']
      retrieve_obj(job_id, archive_id)
    else:
      print(f'Invalid message{data}')
  # Delete message
  try:
    sqs.delete_message(
        QueueUrl=sqs_thaw_url,
        ReceiptHandle=message['handle']
    )
  except ClientError as e:
    print(f'Unable to delete message from sqs: {e}')

def retrieve_obj(job_id, archive_id):
  print(f'Retrieve job {job_id}, archive_id:{archive_id}')
  print('Start expedited retrieval')
  expedited_retrieval_result = init_retrieval_job(job_id, archive_id, 'Expedited')
  if expedited_retrieval_result:
    print('Expedited retrieval success')
    return
  else:
    print('Expedited retrieval failed, start standard retrieval')
    standard_retrieval_result = init_retrieval_job(job_id, archive_id, 'Standard')
    if standard_retrieval_result:
      print('Standard retrieval success')
      return
    else:
      print('Standard retrieval failed')

def init_retrieval_job(job_id, archive_id, tier):
# https://docs.aws.amazon.com/amazonglacier/latest/dev/api-initiate-job-post.html
# Send SNS notification to restore topic
  try: 
    resp = glacier.initiate_job(
        vaultName=valt_name,
        jobParameters={
            'Type': 'archive-retrieval',
            'ArchiveId': archive_id,
            'Description': job_id,
            'SNSTopic': job_restore_topic,
            'Tier': tier
        }
    )
  # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#client-exceptions
  # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
  except glacier.exceptions.InsufficientCapacityException as e:
    print(f'InsufficientCapacity:{e}')
    return False
  except ClientError as e:
    print(f'Unable to retrieve job: {e}')
    return False
  except Exception as e:
    print(e)
    return False


  print(f"job id: {resp['jobId']}, {tier} retrieve success")
  return True

if __name__ == '__main__':  

  # Get handles to resources; and create resources if they don't exist
  sqs = boto3.client('sqs', region_name=region)
  # Poll queue for new results and process them
  while True:
    handle_thaw_queue(sqs=sqs)

### EOF