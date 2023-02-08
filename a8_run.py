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

import boto3
from botocore.client import ClientError


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


bucket_name = 'gas-results'
cnet_id = 'songyuanzheng'

def current_epoch_time():
    t = time.time()
    return int(t)


if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')
        # python Annotools/run.py {filepath}
        file_path = sys.argv[1]
        # "annotation_jobs/{annotation_job_id}/{filename}"
        arr = file_path.split('/')
        job_id = arr[1]
        folder_path = '/'.join(arr[:2])
        input_file = arr[2]
        client = boto3.resource('s3', region_name='us-east-1')
        key_result_file, key_log_file = '', ''
        # 1. Upload the results file to S3 results bucket
        # 2. Upload the log file to S3 results bucket
        for _, _, files in os.walk(folder_path):
            for file in files:
                if file == input_file:
                    continue
                key = f'{cnet_id}/userX/{job_id}/{file}'
                cur_file_path = f'{folder_path}/{file}'
                try:
                    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_file
                    # Example of upload files
                    client.meta.client.upload_file(cur_file_path, bucket_name, key)
                except ClientError as e:
                    logging.error(e)
                    print(e)
                    exit(1)
        # 3. Clean up (delete) local job files
        shutil.rmtree(folder_path)

        # 4. Update 
        dynamoDB = boto3.client('dynamodb', region_name='us-east-1')
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
    else:
        print("A valid .vcf file must be provided as input to this program.")
