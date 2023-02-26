# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import time
import driver
import shutil
import json
import logging

import boto3
from botocore.client import ClientError
from configparser import ConfigParser


class Timer(object):
    """A rudimentary timer for coarse-grained profiling
    """
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")



def current_epoch_time():
    t = time.time()
    return int(t)


cnet_id = 'songyuanzheng'

def main():
    config = ConfigParser(os.environ)
    config.read('ann_config.ini')
    region = config['aws']['AwsRegionName']
    bucket_name = config['s3']['Result_Bucket']



    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')
        # python Annotools/run.py {filepath}
        file_path = sys.argv[1]
        job_id = sys.argv[2]
        user_id = sys.argv[3]
        # "annotation_jobs/{annotation_job_id}/{filename}"
        arr = file_path.split('/')
        folder_path = f"annotation_jobs/{job_id}"
        input_file = arr[2]
        client = boto3.resource('s3', region_name=region)
        key_result_file, key_log_file = '', ''
        # 1. Upload the results file to S3 results bucket
        # 2. Upload the log file to S3 results bucket
        for _, _, files in os.walk(folder_path):
            for file in files:
                if file == input_file:
                    continue
                key = f'{cnet_id}/{user_id}/{job_id}/{file}'
                cur_file_path = f'{folder_path}/{file}'
                if file.endswith('.annot.vcf'):
                    key_result_file = key
                if file.endswith('.log'):
                    key_log_file = key
                try:
                    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_file
                    # Example of upload files
                    client.meta.client.upload_file(cur_file_path, bucket_name, key)
                except ClientError as e:
                    logging.error(e)
                    print(e)
                    exit(1)
        # 3. Clean up (delete) local job files
        try: 
            shutil.rmtree(folder_path)
        except OSError as e:
            print("Error while removing job directory: %s - %s." % (e.filename, e.strerror))
        # 4. Update 
        dynamoDB = boto3.client('dynamodb', region_name=region)
        update_exp = 'SET s3_results_bucket = :bucket, s3_key_result_file = :result_file, ' \
                     's3_key_log_file = :log_file, complete_time = :time, job_status = :status'
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
            # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html#Expressions.UpdateExpressions.SET
            # Examples of AttributeUpdates
            dynamoDB.update_item(TableName="songyuanzheng_annotations",
                                 Key={'job_id': {'S': job_id}},
                                 UpdateExpression=update_exp,
                                 ExpressionAttributeValues={
                                     ':bucket': {'S': bucket_name},
                                     ':result_file': {'S': key_result_file},
                                     ':log_file': {'S': key_log_file},
                                     ':time': {'N': str(current_epoch_time())},
                                     ':status': {'S': 'COMPLETED'}
                                 })
        except ClientError as error:
            logging.error(e)
            print(error)
        # 5. publish a notification that job is finished
        msg = {
            'user_id': user_id,
            'job_id': job_id,
            'complete_time': str(current_epoch_time())
        }
        sns = boto3.client('sns', region_name=region)
        try:
            sns.publish(
                TopicArn=config['sns']['AWS_SNS_ARN_TOPIC'],
                Message=json.dumps(msg)
            )
        except ClientError as e:
            print(f'Fail to post message to sns: {e}')
        # 6. trigger data archive for free users
        msg = {
            'bucket': config['s3']['UploadBucket'],
            'filename': key_result_file,
            'user_id': user_id,
            'job_id': job_id
        }
        data = {
            'seconds': config['sfn']['AVAILABLE_TIME'],
            'notification': {
                'topic': config['sns']['SNSArchiveTopic'],
                'message': json.dumps(msg)
            }
        }
        # Ref:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html#SFN.Client.start_execution
        # https://hands-on.cloud/working-with-step-functions-in-python-using-boto3/#h-execute-step-functions-workflow
        # https://hands-on.cloud/aws-step-functions-tutorial/
        # Examples from the code of step functions
        sfn = boto3.client('stepfunctions', region_name=config['aws']['AwsRegionName'])
        try:
            sfn.start_execution(
                stateMachineArn=config['sfn']['ArchiveStepFunction'],
                name=job_id,
                input=json.dumps(data)
            )
        except ClientError as e:
            print(f'Fail to trigger step function: {e}')


    else:
        print("A valid .vcf file must be provided as input to this program.")

if __name__ == '__main__':
  main()