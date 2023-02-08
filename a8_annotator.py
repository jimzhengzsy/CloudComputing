from flask import Flask, request, jsonify, render_template
from botocore.client import Config, ClientError
import json
import uuid
import os
import subprocess
import boto3
import time

app = Flask(__name__)

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


@app.route('/annotations', methods=['POST'])

# This endpoint allows users to submit an annotation job
def submit_annonations():
  # From Example of The Request Object
  # https://flask.palletsprojects.com/en/2.2.x/quickstart/#a-minimal-application
  # https://flask.palletsprojects.com/en/2.2.x/api/#flask.Request
  # Extract job parameters from request body
  request_body = request.json
  job_id = request_body["job_id"]['S']
  input_file_name = request_body['input_file_name']['S']
  input_bucket = request_body['s3_inputs_bucket']['S']
  input_file_key = request_body['s3_key_input_file']['S']

  # Get the filename
  filename = input_file_name
  annotation_job_id = job_id

  # use the file system for persistence
  # If it's first time to create annotation jobs, create the folder annotation_jobs.
  if not os.path.exists("annotation_jobs"):
    os.makedirs("annotation_jobs")
  os.makedirs(f"annotation_jobs/{annotation_job_id}")
  file_path = f"annotation_jobs/{annotation_job_id}/{filename}"

  # https://ashish.ch/generating-signature-version-4-urls-using-boto3/
  # Initialize s3 client
  s3 = boto3.resource('s3', region_name='us-east-1')
  # example in download file
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_file
  try:
    s3.meta.client.download_file(input_bucket, input_file_key, file_path)
  except ClientError as error:

    logging.error(e)
    error = {
      'code': 500,
      'status': 'error',
      'message': "Fail to download the file"
    }

    return jsonify(error), 500

  # Spawn a subprocess to run the annotator using the provided input file.
  # https://docs.python.org/3/library/subprocess.html
  try:
    subprocess.Popen(["python","anntools/a8_run.py", f"{file_path}"])
  except Exception as e:
    error = {
      'code': 500,
      'status': 'error',
      'message': "Fail to launch the annotator"
    }

    return jsonify(error), 500

  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
  # https://stackoverflow.com/questions/63418641/dynamodb-boto3-conditional-update
  # Update the job status in dynamoDB
  client = boto3.client('dynamodb',region_name='us-east-1')
  try:
    client.update_item(TableName='songyuanzheng_annotations',
                       Key={'job_id': {'S': job_id}},
                       UpdateExpression='SET job_status = :newVal',
                       ConditionExpression='job_status = :oldVal',
                       ExpressionAttributeValues={
                           ':newVal': {'S': 'RUNNING'},
                           ':oldVal': {'S': 'PENDING'}
                       })
  except Exception as e:
    error = {
      'code': 500,
      'status': 'error',
      'message': "Fail to update the job status"
    }

    return jsonify(error), 500
  # construct response
  response = {
    "code": 201,
    "data": {
        "job_id": annotation_job_id,
        "input_file": filename
    } 
  }

  return jsonify(response), 201

@app.route('/annotations/<job_id>', methods=['GET'])
# This endpoint allows users to get the status of an annotation job status and, for completed jobs, the contents of the log file. 
# Job id for testing: 0518e534-2d23-4364-b522-69068776972b
def get_annotation_status(job_id):
  # Since we use file system for persistence, we use os.path to check the job id is valid or not.
  if not os.path.exists(f"annotation_jobs/{job_id}.txt"):
    error = {
      'code': 400,
      'status': 'error',
      'message': "job id does not exists"
    }

    return jsonify(error), 400
    # return jsonify({"Error: job id does not exists"}), 400

  # If the job id exists, we extract the annotated file
  input_file = None
  with open(f"annotation_jobs/{job_id}.txt", "r") as f:
    lines = f.readlines()
    job_id = lines[0]
    input_file = lines[1]
    # Remove .txt
    input_file = input_file[:-4]

  if input_file is None:
    error = {
      'code': 400,
      'status': 'error',
      'message': "annotated file does not exists"
    }

    return jsonify(error), 400
    # return jsonify({"Error: annotated file does not exists"}), 400

  else:
    annotated_file = f"{input_file}.annot.vcf"

    '''
    This endpoint allows users to get the status of an annotation job status and, for completed jobs, the contents of the log file. 
    The annotator generates two final output files for a given <input_filename>.vcf: 
    <input_filename>.annot.vcf: the final annotated file
    <input_filename>.vcf.count.log: a list of variant counts by type, and a total count.
    '''

    # The job complete
    if os.path.exists(f"anntools/data/{annotated_file}"):
      # Read the content of log file
      log_file = f"anntools/data/{input_file}.vcf.count.log"
      with open(log_file, "r") as f:
        log_content = f.read()
      response_completed = {
        "code": 200,
        "data": {
            "job_id": job_id,
            "job_status": "completed",
            "log": log_content
        }
      }

      return jsonify(response_completed)
    # The job is running
    else:
      response_running = {
        "code": 200,
        "data": {
            "job_id": job_id,
            "job_status": "running",
        }
      }
      return jsonify(response_running)

# @app.route('/annotations', methods=['GET'])
# # This endpoint allows the user to get a list of annotation jobs, and optionally the status/log for a job using the link provided. 
# def get_annotation_list():
#   files = os.listdir("annotation_jobs")
#   jobs = []
#   for file in files:

#     job_id = file[:-4]
#     job_endpoint_url = f"http://songyuanzheng-a4.ucmpcs.org:5000/annotations/{job_id}"
#     jobs.append({
#         "job_id": job_id,
#         "job_details": job_endpoint_url
#     })
#   # If annotation job file is not exist.
#   if len(jobs) == 0:
#     error = {
#       'code': 400,
#       'status': 'error',
#       'message': "Job list does not exists"
#     }

#     return jsonify(error), 400

#   response = {
#     "code": 200,
#     "data": {
#         "jobs": jobs
#     }
#   }
#   return jsonify(response), 200

def current_epoch_time():
    t = time.time()
    return int(t)
 

# Run the app server
app.run(host='0.0.0.0', debug=True)
