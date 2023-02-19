from flask import Flask, request, jsonify, render_template
from botocore.client import Config, ClientError
import json
import uuid
import os
import subprocess
import boto3
import time
import jmespath
import logging

from configparser import ConfigParser


def submit_annonations(job):
  # Get configuration
  config = ConfigParser(os.environ)
  config.read('ann_config.ini')

  region = config['aws']['AwsRegionName']
  table_name = config['dynamodb']['AWS_DYNAMODB_ANNOTATIONS_TABLE']

  if job is None:
    return False
  job_id = ''
  input_file_name = ''
  user_id = ''
  input_file_key = ''
  input_bucket = ''

  # Extract job parameters from request body
  # TypeError occur sometimes but not affect annotation works.
  
  try:
    job_id = job['job_id']['S']
    input_file_name = job['input_file_name']['S']
    user_id = job['user_id']['S']
    input_bucket = job['s3_inputs_bucket']['S']
    input_file_key = job['s3_key_input_file']['S']
  except Exception as e:
    logging.error(e)
    return False


  if job_id is None or input_file_name is None or user_id is None:
    print("job_item is not complete")
    print(job)
    return False
  # The job is complete
  # Get the filename
  filename = input_file_name
  annotation_job_id = job_id

  # use the file system for persistence
  # If it's first time to create annotation jobs, create the folder annotation_jobs.
  if not os.path.exists("annotation_jobs"):
    os.makedirs("annotation_jobs")
  if not os.path.exists(f"annotation_jobs/{annotation_job_id}"):
    os.makedirs(f"annotation_jobs/{annotation_job_id}")
  file_path = f"annotation_jobs/{annotation_job_id}/{filename}"

  # https://ashish.ch/generating-signature-version-4-urls-using-boto3/
  # Initialize s3 client
  s3 = boto3.resource('s3', region_name=region)
  # example in download file
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_file
  try:
    s3.meta.client.download_file(input_bucket, input_file_key, file_path)
  except ClientError as error:

    logging.error(e)
    return False

  # Spawn a subprocess to run the annotator using the provided input file.
  # https://docs.python.org/3/library/subprocess.html
  try:
    subprocess.Popen(["python","run.py", f"{file_path}", f"{annotation_job_id}", f"{user_id}"])
  except Exception as e:
    logging.error(e)
    return False

  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
  # https://stackoverflow.com/questions/63418641/dynamodb-boto3-conditional-update
  # Update the job status in dynamoDB
  client = boto3.client('dynamodb',region_name=region)
  try:
    client.update_item(TableName=table_name,
                       Key={'job_id': {'S': job_id}},
                       UpdateExpression='SET job_status = :newVal',
                       ConditionExpression='job_status = :oldVal',
                       ExpressionAttributeValues={
                           ':newVal': {'S': 'RUNNING'},
                           ':oldVal': {'S': 'PENDING'}
                       })
  except Exception as e:
    logging.error(e)
    return False

  return True

if __name__ == "__main__":
  config = ConfigParser(os.environ)
  config.read('ann_config.ini')

  region = config['aws']['AwsRegionName']
  table_name = config['dynamodb']['AWS_DYNAMODB_ANNOTATIONS_TABLE']
  topic = config['sns']['AWS_SNS_ARN_TOPIC']
  queue = config['sqs']['AWS_SQS_QUEUE_URL']
  
  if not os.path.exists("annotation_jobs"):
    os.makedirs("annotation_jobs")
  # Connect to SQS and get the message queue
  sqs = boto3.client('sqs', region_name=region)
  # Poll the message queue in a loop 
  while True:
      # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html
      # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message

      # Attempt to read a message from the queue
      # Use long polling - DO NOT use sleep() to wait between polls
      try:
          resp = sqs.receive_message(
              QueueUrl=queue,
              WaitTimeSeconds=20
          )
      except ClientError as e:
          logging.error(e)
          continue

      response = jmespath.search('Messages[*].{handle: ReceiptHandle, body: Body}', resp)


      if response is None:

        print("Response is empty, waiting for sqs receive message")
        continue
      for message in response:
        try:
          data = json.loads(message['body'])
          job_item = json.loads(data['Message'])
          print('')
          print(f"Job_item received from annotator:{job_item}")
          print('')
        except json.decoder.JSONDecodeError as e:
            print(f'Fail to decode message: {message["body"]} {e}')
            continue


      
      # Delete the message from the queue, if job was successfully submitted
      if 'job_id' not in job_item or 'user_id' not in job_item:
        print("job_item is not complete")
        print(job_item)
        continue

      if job_item is None:
        print('job_item is None')
        continue

      print('')
      print(f"Job_item received from annotator before submit_annonations:{job_item}")
      print('')
      status = submit_annonations(job_item)
      if not status:
          continue
      print('')
      print(f"Job_item received from annotator submit_annonations :{job_item}")
      print('')
      # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.delete_message
      try:
          response = sqs.delete_message(
              QueueUrl=queue,
              ReceiptHandle=message['handle']
          )
      except ClientError as e:
          logging.error(e)