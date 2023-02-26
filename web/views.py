# views.py
#
# Copyright (C) 2011-2022 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime, timedelta, timezone

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template, 
  request, session, url_for, jsonify)

from app import app, db
from decorators import authenticated, is_premium
import logging

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Open a connection to the S3 service
  s3 = boto3.client('s3', 
    region_name=app.config['AWS_REGION_NAME'], 
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + "/job"

  # Define policy conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f'Unable to generate presigned URL for upload: {e}')
    return abort(500)

  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html',
    s3_post=presigned_post,
    role=session['role'])


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  region = app.config['AWS_REGION_NAME']
  table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
  topic = app.config['AWS_SNS_ARN_TOPIC']

  # Parse redirect URL query parameters for S3 object info
  # From Example of The Request Object
  # https://flask.palletsprojects.com/en/2.2.x/quickstart/#a-minimal-application
  # https://flask.palletsprojects.com/en/2.2.x/api/#flask.Request
  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket = request.args.get('bucket')
  s3_key = request.args.get('key')


  # Extract job_id and filename from the key
  user_id, job_id, filename = parse_s3_key(s3_key)

  # Persist job to database
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.put_item
  # Example of the format of item
  # Create a job item and persist it to the annotations database
  data = {  
          "job_id": {'S': job_id},
          "user_id": {'S': user_id},
          "input_file_name": {'S': filename},
          "s3_inputs_bucket": {'S': bucket},
          "s3_key_input_file": {'S': s3_key},
          "submit_time": {'N': str(current_epoch_time())},
          "job_status": {'S': 'PENDING'}
       }    
  client = boto3.client('dynamodb', region_name=region)
  try:
      client.put_item(TableName=table_name,Item=data)
  except ClientError as e:
      app.logger.error(f'Fail to update data to dynamodb: {e}')
      return abort(500)

  # Send message to request queue
  # publish a notification message to SNS
  # Ref:
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
  # https://stackoverflow.com/questions/34029251/aws-publish-sns-message-for-lambda-function-via-boto3-python2
  # https://github.com/aws-samples/amazon-rekognition-video-analyzer/issues/41
  sns = boto3.client('sns', region_name=region)
  try:
      resp = sns.publish(
          TopicArn=topic,
          Message=json.dumps(data)
      )
  except ClientError as e:
      logging.error(e)
      error = {
        'code': 500,
        'status': 'error',
        'message': "Fail to publish message to SNS"
      }

      return jsonify(error), 500
  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():

  # Get list of annotations to display
  region = app.config['AWS_REGION_NAME']
  table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
  table_index = app.config['AWS_DYNAMODB_ANNOTATIONS_INDEX']
  topic = app.config['AWS_SNS_ARN_TOPIC']
  query_fields = 'job_id,submit_time,input_file_name,job_status'
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
  # Example of query
  dynamodb = boto3.client('dynamodb', region_name=region)
  try:
    response = dynamodb.query(
            TableName=table_name,
            IndexName=table_index,
            ProjectionExpression=query_fields,
            KeyConditionExpression='user_id = :v1',
            ExpressionAttributeValues={':v1': {'S': get_user_id()}}
      )
  except ClientError as e:
    app.logger.error(f'Fail to query annotations from dynamodb: {e}')

    return abort(500)

  items = response['Items']
  for item in items:
    item['job_id']['link'] = f"{request.url}/{item['job_id']['S']}"
    item['submit_time']['N'] = parse_timestamp(item['submit_time']['N'])
  return render_template('annotations.html', annotations=items)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<job_id>', methods=['GET'])
@authenticated
def annotation_details(job_id):
  region = app.config['AWS_REGION_NAME']
  table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
  table_index = app.config['AWS_DYNAMODB_ANNOTATIONS_INDEX']
  topic = app.config['AWS_SNS_ARN_TOPIC']

  dynamodb = boto3.client('dynamodb', region_name=region)
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.get_item
  # Examples of get_item
  try:
    response = dynamodb.get_item(
          TableName=table_name,
          Key={'job_id': {'S':job_id}}
      )
  except ClientError as e:
    app.logger.error(f'Fail to query annotation job {job_id}from dynamodb: {e}')

    return abort(500)

  job_item = response['Item']

  if job_item['user_id']['S'] != get_user_id():
    app.logger.error(f'You are not authorized user for this annotation job {job_id}: {e}')
    return abort(403)

  job_data = {
    'Request ID:': {'val': job_item['job_id']['S']},
    'Request Time:': {'val': parse_timestamp(job_item['submit_time']['N'])},
    'VCF Input File:': {'val': job_item['input_file_name']['S'], 
                        'link': generate_download_link(job_item['s3_inputs_bucket']['S'], job_item['s3_key_input_file']['S'])},
    'Status:': {'val': job_item['job_status']['S']}
  }
  job_results = {}
  if job_item['job_status']['S'] == 'COMPLETED':
    job_data['Complete Time:'] = {'val':parse_timestamp(job_item['complete_time']['N'])}

    if 'results_file_archive_id' in job_item:
      if get_user_role() == 'premium_user':
        job_results['Annotated Results File:'] = {'val': 'file is being restored; please check back later'}
      else:
        job_results['Annotated Results File:'] = {'val': 'upgrade to Premium for download', 'link': '/subscribe'}
    else:
      job_results['Annotated Results File:'] = {'val': 'download', 'link':
                                                generate_download_link(job_item['s3_results_bucket']['S'], job_item['s3_key_result_file']['S'])}
  
    job_results['Annotation Log File'] = {
        'val': 'view', 'link': f'{request.url}/log'}
  return render_template('annotation.html', job_data=job_data, job_results=job_results)


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<job_id>/log', methods=['GET'])
@authenticated
def annotation_log(job_id):
  region = app.config['AWS_REGION_NAME']
  table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
  table_index = app.config['AWS_DYNAMODB_ANNOTATIONS_INDEX']
  topic = app.config['AWS_SNS_ARN_TOPIC']

  dynamodb = boto3.client('dynamodb', region_name=region)
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.get_item
  # Examples of get_item
  try:
    response = dynamodb.get_item(
          TableName=table_name,
          Key={'job_id': {'S':job_id}}
      )
  except ClientError as e:
    app.logger.error(f'Fail to query annotation job {job_id}from dynamodb: {e}')

    return abort(500)

  job_item = response['Item']

  # Check authentication
  if job_item['user_id']['S'] != get_user_id():
    app.logger.error(f'You are not authorized user for this annotation job {job_id}: {e}')
    return abort(403)
  # Check job completion
  if job_item['job_status']['S'] != 'COMPLETED':
    return abort(401)

  bucket = job_item['s3_results_bucket']['S']
  log_key = job_item['s3_key_log_file']['S']
  try:
    s3 = boto3.resource('s3', region_name=region)
    log_object = s3.Object(bucket, log_key)
    log = log_object.get()['Body'].read().decode('utf-8')
  except ClientError as e:
    app.logger.error(f'Fail to get log for job: {job_id}: {e}')
    return abort(500)
  return render_template('view_log.html', log=log, job_id=job_id)

"""Subscription management handler
"""
import stripe
from auth import update_profile

@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    pass
    
  elif (request.method == 'POST'):
    # Process the subscription request

    # Create a customer on Stripe

    # Subscribe customer to pricing plan

    # Update user role in accounts database

    # Update role in the session

    # Request restoration of the user's data from Glacier
    # ...add code here to initiate restoration of archived user data
    # ...and make sure you handle files not yet archived!

    # Display confirmation page
    pass

'''
helper functions
'''
def current_epoch_time():
  return int(time.time())

def get_user_id():
  return session['primary_identity']
def get_user_role():
  return session['role']
def parse_timestamp(ts):
  ts = int(ts)
  td = timedelta(hours=app.config['DISPLAY_TIME_ZONE'])
  tz = timezone(td)
  dt = datetime.fromtimestamp(ts, tz)
  dt = dt.strftime('%Y-%m-%d %H:%M:%S')
  return f'{dt} CST'
def parse_s3_key(key: str):
    # prefix/user_id/job_id~filename
    arr = key.split('/', 2)
    user_id = arr[1]
    idx = arr[2].index('~')
    job_id = arr[2][:idx]
    filename = arr[2][idx+1:]
    return user_id, job_id, filename
    
def generate_download_link(bucket, key):
  region = app.config['AWS_REGION_NAME']
  s3 = boto3.client('s3', region_name=region)
  try:
      response = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key})
  except ClientError as e:
      app.logger.error(f'Unable to generate download link: {e}')
      return None
  return response
"""Set premium_user role
"""
@app.route('/make-me-premium', methods=['GET'])
@authenticated
def make_me_premium():
  # Hacky way to set the user's role to a premium user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="premium_user"
  )
  return redirect(url_for('profile'))


"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF