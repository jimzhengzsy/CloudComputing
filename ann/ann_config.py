# ann_config.py
#
# Copyright (C) 2011-2022 Vas Vasiliadis
# University of Chicago
#
# Set GAS annotator configuration options
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

class Config(object):

  # Get configuration
  from configparser import ConfigParser
  config = ConfigParser(os.environ)
  config.read('app_config.ini')

  CSRF_ENABLED = True

  ANNOTATOR_BASE_DIR = "/home/ubuntu/gas/ann/"
  ANNOTATOR_JOBS_DIR = "/home/ubuntu/gas/ann/jobs"

  AWS_REGION_NAME = "us-east-1"

  # AWS S3 upload parameters
  AWS_S3_INPUTS_BUCKET = "gas-inputs"
  AWS_S3_RESULTS_BUCKET = "gas-results"

  # AWS SNS topics

  # AWS SQS queues
  AWS_SQS_WAIT_TIME = 20
  AWS_SQS_MAX_MESSAGES = 10

  # AWS DynamoDB
  AWS_DYNAMODB_ANNOTATIONS_TABLE = "songyuanzheng_annotations"

  # AWS SNS topic for A10
  AWS_SNS_ARN_TOPIC_A10 = 'arn:aws:sqs:us-east-1:127134666975:songyuanzheng_a10_job_requests'
  # AWS SNS topic
  AWS_SNS_ARN_TOPIC = 'arn:aws:sns:us-east-1:127134666975:songyuanzheng_a14_job_requests'
  # AWS SQS queue 
  AWS_SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/127134666975/songyuanzheng_a14_job_requests'
  # AWS SQS queue for A10
  AWS_SQS_Queue_NAME_A10 = 'https://sqs.us-east-1.amazonaws.com/127134666975/songyuanzheng_a10_job_requests'

### EOF