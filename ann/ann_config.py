# ann_config.ini
#
# Copyright (C) 2011-2022 Vas Vasiliadis
# University of Chicago
#
# GAS annotator configuration
#
##

# AnnTools settings
[ann]

# AWS general settings
[aws]
AwsRegionName = us-east-1

# AWS SQS queues
[sqs]
# AWS SQS queue for A10
AWS_SQS_Queue_NAME_A10 = https://sqs.us-east-1.amazonaws.com/127134666975/songyuanzheng_a10_job_requests
# AWS SQS queue for A11
AWS_SQS_QUEUE_URL_A11 = https://sqs.us-east-1.amazonaws.com/127134666975/songyuanzheng_a11_job_requests

# AWS SQS queue 
AWS_SQS_QUEUE_URL = https://sqs.us-east-1.amazonaws.com/127134666975/songyuanzheng_a13_job_requests

# AWS S3
[s3]
Result_Bucket = gas-results
# AWS SNS topics
[sns]
# AWS SNS topic for A10
AWS_SNS_ARN_TOPIC_A10 = arn:aws:sns:us-east-1:127134666975:songyuanzheng_a10_job_requests
# AWS SNS topic for A11
AWS_SNS_ARN_TOPIC_A11 = arn:aws:sns:us-east-1:127134666975:songyuanzheng_a11_job_requests

# AWS SNS topic
AWS_SNS_ARN_TOPIC = arn:aws:sns:us-east-1:127134666975:songyuanzheng_a13_job_requests
# AWS DynamoDB
[dynamodb]
AWS_DYNAMODB_ANNOTATIONS_TABLE = songyuanzheng_annotations



### EOF