from flask import Flask, request
import os
import boto3
import json
import time
import threading

app = Flask(__name__)

sqs = boto3.client('sqs', region_name='us-east-1')
request_queue_url = 'https://sqs.us-east-1.amazonaws.com/851725506870/1224979548-req-queue'
response_queue_url = 'https://sqs.us-east-1.amazonaws.com/851725506870/1224979548-res-queue'

s3 = boto3.client('s3', region_name='us-east-1')
input_bucket = '1224979548-in-bucket'
output_bucket = '1224979548-out-bucket'

ec2 = boto3.client('ec2', region_name='us-east-1')
app_tier_ami_id = 'ami-0c76b1580b7c0900f'
instance_type = 't2.micro'
key_name = 'sarfraz_key'


MIN_INSTANCES = 1
MAX_INSTANCES = 20


@app.route('/', methods=['GET'])
def home():
    return 'Hello, World!', 200


@app.route('/', methods=['POST'])
def upload_file():
    if 'inputFile' not in request.files:
        return 'No file part', 400
    
    files = request.files['inputFile']
    
    if files.filename == '':
        return "Error: No file selected", 400
    
    filename = files.filename
    s3.upload_fileobj(files, input_bucket, filename)
    
    message = {
        'filename': filename
    }
    
    sqs.send_message(
        QueueUrl=request_queue_url,
        MessageBody=json.dumps(message)
    )
    
    processed_result = poll_response_queue(filename)
    
    if processed_result:
        # try:
        #     s3_response = s3.get_object(Bucket=output_bucket, Key=filename)
        #     output_result = s3_response['Body'].read().decode('utf-8')

        #     formatted_response = f"{filename}:{output_result}"
        #     return formatted_response, 200
        
        # except Exception as e:
        #     return f"No processed result found for {filename}"
        return f"{filename}:{processed_result[1]}", 200
    else:
        return f"Error processing {filename}", 500
    


def poll_response_queue(filename):
    while True:
        response = sqs.receive_message(
            QueueUrl=response_queue_url,
            MaxNumberOfMessages=5,
            WaitTimeSeconds=1
        )
        
        if 'Messages' in response:
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            
            body = json.loads(message['Body'])
            processed_filename = body.get('filename')
            result = body.get('result')
            
            if processed_filename == filename:
                sqs.delete_message(
                    QueueUrl=response_queue_url,
                    ReceiptHandle=receipt_handle
                )
                
                return (processed_filename, result)
            
        time.sleep(2)
    
    return None


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, threaded=True)