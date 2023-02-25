# annotator_webhook.py
#
# NOTE: This file lives on the AnnTools instance
# Modified to run as a web server that can be called by SNS to process jobs
# Run using: python annotator_webhook.py
#
# Copyright (C) 2011-2022 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import requests
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

app = Flask(__name__)
environment = 'ann_config.Config'
app.config.from_object(environment)

region = app.config['AWS_REGION_NAME']
queue = app.config['AWS_SQS_QUEUE_URL']
table_name =app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']

# Connect to SQS and get the message queue
sqs = boto3.client('sqs', region_name=region)
# Check if requests queue exists, otherwise create it
if not os.path.exists("annotation_jobs"):
  os.makedirs("annotation_jobs")




'''
A13 - Replace polling with webhook in annotator

Receives request from SNS; queries job queue and processes message.
Reads request messages from SQS and runs AnnTools as a subprocess.
Updates the annotations database with the status of the request.
'''
@app.route('/process-job-request', methods=['GET', 'POST'])
def annotate():
  print(request)
  if (request.method == 'GET'):
    return jsonify({
      "code": 405, 
      "error": "Expecting SNS POST request."
    }), 405

  # https://docs.aws.amazon.com/sns/latest/dg/sns-subscribe-https-s-endpoints-to-topic.html
  # https://docs.amazonaws.cn/en_us/sns/latest/dg/SendMessageToHttp.prepare.html
  # https://docs.amazonaws.cn/en_us/sns/latest/dg/sns-message-and-json-formats.html#http-subscription-confirmation-json


  # Check message type

  message_type = request.headers['x-amz-sns-message-type']
  app.logger.info(message_type)
  raw_data = request.data
  body = json.loads(raw_data.decode())
  if (message_type == 'SubscriptionConfirmation'):
    # Confirm SNS topic subscription confirmation
    try:
        resp = requests.get(body['SubscribeURL'])
    except Exception as e:
        app.logger.error(f'Unable to confirm subscription: {e}')
        return error(500, "Fail to confirm subscription")
    app.logger.info(resp)
    return jsonify({
    "code": 200, 
    "message": "Subscription confirmed."
  }), 200

  # Process job request notification
  elif (message_type == 'Notification'):
    # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message

    # Attempt to read a message from the queue
    annotations()
    return jsonify({
      "code": 200, 
      "message": "Annotation job request processed."
    }), 200
  else:
    return jsonify({
      "code": 400, 
      "message": "Fail to process the job request."
    }), 400


def annotations():
  if sqs is None:
    app.logger.error('sqs does not exists')
    return
  try:
    resp = sqs.receive_message(
        QueueUrl=queue,
        WaitTimeSeconds=20
    )
  except ClientError as e:
    logging.error(e)
    app.logger.error(f"Fail to receive messages from sqs:{e}")
    return

  response = jmespath.search('Messages[*].{handle: ReceiptHandle, body: Body}', resp)
  if response is None:
    app.logger.info("The message is empty")
    return 
  for message in response:
    try:
      data = json.loads(message['body'])
      job = json.loads(data['Message'])
    except json.decoder.JSONDecodeError as e:
      print(f'Fail to decode message: {message["body"]} {e}')
      continue
    # Extract job parameters from request body
    # TypeError occur sometimes but not affect annotation works.
    if job is None:
      return 
    job_id = job['job_id']['S']
    input_file_name = job['input_file_name']['S']
    user_id = job['user_id']['S']
    input_bucket = job['s3_inputs_bucket']['S']
    input_file_key = job['s3_key_input_file']['S']



    # The job is complete
    # Get the filename
    filename = input_file_name
    annotation_job_id = job_id

    status = submit_job(annotation_job_id, user_id, input_bucket, input_file_key, input_file_name)
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
    # https://stackoverflow.com/questions/63418641/dynamodb-boto3-conditional-update
    # Update the job status in dynamoDB
    client = boto3.client('dynamodb',region_name=region)
    try:
      client.update_item(TableName=table_name,
                         Key={'job_id': {'S': annotation_job_id}},
                         UpdateExpression='SET job_status = :newVal',
                         ConditionExpression='job_status = :oldVal',
                         ExpressionAttributeValues={
                             ':newVal': {'S': 'RUNNING'},
                             ':oldVal': {'S': 'PENDING'}
                         })
    except Exception as e:
      logging.error(e)
      continue
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.delete_message
    try:
      response = sqs.delete_message(
          QueueUrl=queue,
          ReceiptHandle=message['handle']
      )
    except ClientError as e:
      logging.error(e)

def submit_job(annotation_job_id, user_id, bucket, input_file_key, input_file_name):
  # use the file system for persistence
  # If it's first time to create annotation jobs, create the folder annotation_jobs.
  if not os.path.exists("annotation_jobs"):
    os.makedirs("annotation_jobs")
  if not os.path.exists(f"annotation_jobs/{annotation_job_id}"):
    os.makedirs(f"annotation_jobs/{annotation_job_id}")
  file_path = f"annotation_jobs/{annotation_job_id}/{input_file_name}"

  # https://ashish.ch/generating-signature-version-4-urls-using-boto3/
  # Initialize s3 client
  s3 = boto3.resource('s3', region_name=region)
  # example in download file
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_file
  try:
    s3.meta.client.download_file(bucket, input_file_key, file_path)
  except ClientError as error:
    logging.error(e)
  # Spawn a subprocess to run the annotator using the provided input file.
  # https://docs.python.org/3/library/subprocess.html
  try:
    subprocess.Popen(["python","run.py", f"{file_path}", f"{annotation_job_id}", f"{user_id}"])
  except Exception as e:
    logging.error(e)

  return True

app.run('0.0.0.0', debug=True)

### EOF