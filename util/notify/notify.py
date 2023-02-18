# notify.py
#
# Notify users of job completion
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
import jmespath
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('notify_config.ini')

'''Capstone - Exercise 3(d)
Reads result messages from SQS and sends notification emails.
'''
def handle_results_queue(sqs=None):
  region = config['aws']['AwsRegionName']
  queue_url = config['sqs']['SqsUrl']
  if sqs is None:
    sqs = boto3.client('sqs', region_name=region)
  # Read a message from the queue
  try:
    response = sqs.receive_message(
         QueueUrl=queue_url,
         WaitTimeSeconds=20 
      )
  except ClientError as e:
      print(f'Fail to receive message from sqs: {e}')
      return

  response = jmespath.search('Messages[*].{handle: ReceiptHandle, body: Body}', response)
  if response is None:
      return

  for message in response:
      try:
          data = json.loads(message['body'])
          data = json.loads(data['Message'])
      except json.decoder.JSONDecodeError as e:
          print(f'Fail to decode message: {message["body"]} {e}')
          continue
  # Process message

  if 'job_id' not in data or 'user_id' not in data or \
          'complete_time' not in data:
      print(f'Invalid message {e}')

  job_id = data['job_id']
  user_id = data['user_id']
  complete_time = parse_timestamp(data['complete_time'])
  link = f"{config['gas']['GasAnnotation']}/{job_id}"

  subject = config['email']['Subject'] % job_id
  body = config['email']['Body'] % (complete_time, link)

  try:
      user_profile = helpers.get_user_profile(user_id)
      helpers.send_email_ses(
          recipients=user_profile['email'],
          subject=subject,
          body=body
      )
  except Exception as e:
      print(f'Fail to get user profile or send email: {e}')

  # Delete message
  try:
      response = sqs.delete_message(
          QueueUrl=queue_url,
          ReceiptHandle=message['handle']
      )
  except ClientError as e:
      print(f'Fail to delete message from sqs: {e}')



def parse_timestamp(ts):
    ts = int(ts)
    td = timedelta(hours=-6)
    tz = timezone(td)
    dt = datetime.fromtimestamp(ts, tz)
    dt = dt.strftime('%Y-%m-%d %H:%M:%S')
    return f'{dt} CST'


if __name__ == '__main__':
  
  # Get handles to resources; and create resources if they don't exist
  region = config['aws']['AwsRegionName']
  sqs = boto3.client('sqs', region_name=region)
  # Poll queue for new results and process them
  while True:
    handle_results_queue(sqs=sqs)

### EOF
