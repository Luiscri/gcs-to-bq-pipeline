import base64
import json
from datetime import datetime
import os
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

def start_job(event, context):
  message = base64.b64decode(event['data']).decode('utf-8')
  message = json.loads(message)
  bucket = message['bucket']
  filename = message['name']
  PROJECT_ID = os.getenv('PROJECT_ID')
  REGION = os.getenv('REGION')
  DATAFLOW_BUCKET = os.getenv('DATAFLOW_BUCKET')
  TEMPLATE_NAME = os.getenv('TEMPLATE_NAME')

  source_file = 'gs://{}/{}'.format(bucket, filename)
  now = datetime.now().strftime('%Y%m%d-%H%M%S')

  if filename.startswith('raw/'):
    credentials = GoogleCredentials.get_application_default()
    service = build('dataflow', 'v1b3', credentials=credentials)
    output_prefix = 'gs://{}/processed/{}'.format(bucket, now)
    template = 'gs://{}/templates/{}'.format(DATAFLOW_BUCKET, TEMPLATE_NAME)
    BODY = {
      'jobName': 'process-raw',
      'gcsPath': template,
      'parameters': {
        'input_file' : source_file,
        'output_prefix': output_prefix
      },
      'environment': {
        'tempLocation': 'gs://{}/temp'.format(DATAFLOW_BUCKET)
      }
    }

    req = service.projects().locations().templates().create(
      projectId=PROJECT_ID,
      location=REGION,
      body=BODY
    )
    res = req.execute()
    print('Job sent to Dataflow. Response:')
    print(res)
  else:
    print('File uploaded to unknow directory: {}'.format(filename))