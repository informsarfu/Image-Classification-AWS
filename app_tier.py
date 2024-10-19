import boto3
import json
import time
import os
import subprocess

sqs = boto3.client('sqs', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

request_queue_url = 'https://sqs.us-east-1.amazonaws.com/851725506870/1224979548-req-queue'
response_queue_url = 'https://sqs.us-east-1.amazonaws.com/851725506870/1224979548-res-queue'

input_bucket = '1224979548-in-bucket'
output_bucket = '1224979548-out-bucket'

def download_image_from_s3(filename):
    local_path = f"/tmp/{filename}"
    s3.download_file(input_bucket, filename, local_path)
    return local_path

def upload_result_to_s3(key, result):
    s3.put_object(
        Bucket=output_bucket,
        Key=key,
        Body=result
    )

def process_image(local_path):
    try:
        result = subprocess.check_output(['python3', 'face_recognition.py', local_path])
        return result.decode('utf-8').strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running face recognition: {e}")
        return None

while True:
    response = sqs.receive_message(
        QueueUrl=request_queue_url,
        MaxNumberOfMessages=5,
        WaitTimeSeconds=1
    )
    
    if 'Messages' in response:
        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        
        body = json.loads(message['Body'])
        filename = body.get('filename')
        
        local_image_path = download_image_from_s3(filename)
        
        recognition_result = process_image(local_image_path)
        
        if recognition_result:
            upload_result_to_s3(filename, recognition_result)

            response_message = {
                'filename': filename
            }
            
            sqs.send_message(
                QueueUrl=response_queue_url,
                MessageBody=json.dumps(response_message)
            )
        
        sqs.delete_message(
            QueueUrl=request_queue_url,
            ReceiptHandle=receipt_handle
        )
        
    time.sleep(5)
