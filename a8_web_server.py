from flask import Flask, request, jsonify, render_template
from botocore.client import Config, ClientError
import json
import uuid
import os
import subprocess
import boto3
import time
import requests

app = Flask(__name__)

def current_epoch_time():
    t = time.time()
    return int(t)

@app.route('/', methods=['GET'])
# This method is called when a user visits "/" on your server
def home():
  # substitute your favorite geek message below!
  return "ZORK I: The Great Underground Empire."

@app.route('/hello', methods=['GET'])
# This method is called when a user visits "/hello" on your server
def hello():
  # Another geeky message here!
  return "West of House<br ><br />&gt; _"

@app.route('/annotate', methods=['GET'])
# Create a signed policy for AWS POST request
# Passes the signed policy (and any other required information) to the form template and renders the form.
def annotate():
    # Define S3 policy fields and conditions
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/amazon-s3-policy-keys.html
    # Examples of Amazon S3 condition keys for object operations
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-bucket-policies.html
    # Example of Set a bucket policy
    bucket_name = 'gas-inputs'

    # Create unique filename for each object(uploaded file)
    unique_file_id = str(uuid.uuid4())
    unique_filename = unique_file_id + "~${filename}"
    key = 'songyuanzheng/userX/' + unique_filename

    # Get URL from the request and set the redirect_url dynamically
    request_url = request.url
    redirect_url = f'{request_url}/job'

    conditions = [
        {"acl": "private"},
        {"bucket": "gas-inputs"},
        # {"expires": expire_time},
        {"success_action_redirect": redirect_url},
        {"success_action_status": "201"}
    ]
    fields = {
        "acl": "private", 
        "bucket": "gas-inputs", 
        # "expires": str(expire_time), 
        "success_action_redirect": redirect_url, 
        "success_action_status": "201"
    }

    # https://ashish.ch/generating-signature-version-4-urls-using-boto3/
    # Initialize the client
    s3 = boto3.client('s3', region_name='us-east-1', config=Config(signature_version='s3v4'))

    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
    # Example of Generating a presigned URL to upload a file
    # Generate signed POST request

    try:
        data = s3.generate_presigned_post(Bucket=bucket_name,
                                           Key=key,
                                           Fields=fields,
                                           Conditions=conditions,
                                           ExpiresIn=60)
    except ClientError as e:
        logging.error(e)
        error = {
          'code': 500,
          'status': 'error',
          'message': "Generate presigned post failed"
        }

        return jsonify(error), 500



    # Render the upload form template
    return render_template("annotate.html", filename=unique_filename, data=data)

@app.route('/annotate/files', methods=['GET'])
# Create a signed policy for AWS POST request
# Passes the signed policy (and any other required information) to the form template and renders the form.
def annotate_files():
    bucket_name = 'gas-inputs'

    # https://ashish.ch/generating-signature-version-4-urls-using-boto3/
    # Initialize the client with region name
    s3 = boto3.client('s3', region_name='us-east-1')

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html?highlight=list_objects_v2#S3.Client.list_objects_v2
    # Example in list_objects_v2
    try:
        result = s3.list_objects_v2(Bucket=bucket_name,Prefix='songyuanzheng/')
        files = [content['Key'] for content in result.get('Contents',[])]
    except ClientError as e:
        logging.error(e)
        error = {
          'code': 500,
          'status': 'error',
          'message': "Fail to list files"
        }

        return jsonify(error), 500



    response = {
      "code": 200,
      "data": {
          "files": files
      }
    }
    return jsonify(response), 200

@app.route("/annotate/job", methods=['GET'])
def annotate_job():

    # From Example of The Request Object
    # https://flask.palletsprojects.com/en/2.2.x/quickstart/#a-minimal-application
    # https://flask.palletsprojects.com/en/2.2.x/api/#flask.Request
    # Get bucket name, key, and job ID from the S3 redirect URL
    bucket = request.args['bucket']
    key = request.args['key']

    # Extract job_id and filename from the key
    arr = key.split('/',2)
    index = arr[2].index('~')
    job_id = arr[2][:index]
    filename = arr[2][index+1:]

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.put_item
    # Example of the format of item
    # Create a job item and persist it to the annotations database
    data = {  
            "job_id": {'S': job_id},
            "user_id": {'S': 'userX'},
            "input_file_name": {'S': filename},
            "s3_inputs_bucket": {'S': bucket},
            "s3_key_input_file": {'S': key},
            "submit_time": {'N': str(current_epoch_time())},
            "job_status": {'S': 'PENDING'}
         }    
    client = boto3.client('dynamodb', region_name='us-east-1')
    try:
        client.put_item(TableName='songyuanzheng_annotations',Item=data)
    except ClientError as e:
        logging.error(e)
        error = {
          'code': 500,
          'status': 'error',
          'message': "Fail to update data to dynamodb"
        }

        return jsonify(error), 500

    # POST job request to the annotator
    ann_job_response = requests.post("http://songyuanzheng-a8-ann.ucmpcs.org:5000/annotations", json=data)

    return ann_job_response.json()


 

  
  


# Run the app server
app.run(host='0.0.0.0', debug=True)
