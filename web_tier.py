from flask import Flask, request
import os
import boto3
import json
import time
import threading

app = Flask(__name__)

#initialize sqs
sqs = boto3.client('sqs', region_name='us-east-1')
request_queue_url = 'https://sqs.us-east-1.amazonaws.com/851725506870/1224979548-req-queue'
response_queue_url = 'https://sqs.us-east-1.amazonaws.com/851725506870/1224979548-res-queue'

#initialize s3
s3 = boto3.client('s3', region_name='us-east-1')
input_bucket = '1224979548-in-bucket'
output_bucket = '1224979548-out-bucket'

#initialize ec2
ec2 = boto3.client('ec2', region_name='us-east-1')
# app_tier_ami_id = 'ami-04968f5e3fbc324b8'
app_tier_ami_id = 'ami-017f20dca259f89e5'
instance_type = 't2.micro'
key_name = 'sarfraz_key'

#scaling limits
MIN_INSTANCES = 0
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
            MaxNumberOfMessages=1,
            WaitTimeSeconds=2
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
            
        time.sleep(0.5)
    
    return None


def get_request_queue_length():
    response = sqs.get_queue_attributes(
        QueueUrl=request_queue_url,
        AttributeNames=['ApproximateNumberOfMessages']
    )
    return int(response['Attributes']['ApproximateNumberOfMessages'])

def get_running_app_tier_instances():
    response = ec2.describe_instances(
        Filters=[
            {
                'Name': 'instance-state-name',
                'Values': ['running']
            },
            {
                'Name': 'tag:Name',
                'Values': ['app-tier-instance']
            }
        ]
    )
    return [instance['InstanceId'] for reservation in response['Reservations'] for instance in reservation['Instances']]

def launch_app_tier_instance():
    user_data_script = '''#!/bin/bash
    # Navigate to the home directory
    cd /home/ubuntu

    # Run app_tier.py in the background and redirect output to a log file
    sudo nohup python3 app_tier.py > app_tier.log 2>&1 &
    '''
    
    response = ec2.run_instances(
        ImageId=app_tier_ami_id,
        InstanceType=instance_type,
        KeyName=key_name,
        MinCount=1,
        MaxCount=1,
        SecurityGroupIds=['sg-09dabf0b0bce945e8'],
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [
                    {
                        'Key': 'Name',
                        'Value': 'app-tier-instance'
                    }
                ]
            }
        ],
        UserData=user_data_script,
        IamInstanceProfile={
            'Name': 'FullAccessIAM'
        }
    )
    print("Launched new App Tier instance:", response['Instances'][0]['InstanceId'])

def terminate_app_tier_instance(instance_id):
    ec2.terminate_instances(InstanceIds=[instance_id])
    print("Terminated App Tier instance:", instance_id)

def scale_app_tier():
    # Get the current number of messages in the SQS queue
    queue_length = get_request_queue_length()
    print(f"Current Request Queue Length: {queue_length}")
    
    # Get the number of running App Tier instances
    running_instances = get_running_app_tier_instances()
    current_instance_count = len(running_instances)
    print(f"Current Running App Tier Instances: {current_instance_count} \n")
    
    # Determine the desired number of instances based on the queue length
    desired_instance_count = min(MAX_INSTANCES, (queue_length + 4) // 5)  # 1 instance for every 5 requests

    # Scale Up
    if current_instance_count < desired_instance_count:
        instances_to_launch = desired_instance_count - current_instance_count
        print(f"Scaling Up: Launching {instances_to_launch} new App Tier instance(s)")
        for _ in range(instances_to_launch):
            launch_app_tier_instance()
    
    # Scale Down
    elif current_instance_count > desired_instance_count:
        instances_to_terminate = current_instance_count - desired_instance_count
        print(f"Scaling Down: Terminating {instances_to_terminate} App Tier instance(s)")
        for _ in range(instances_to_terminate):
            if running_instances:
                instance_to_terminate = running_instances.pop(0)
                terminate_app_tier_instance(instance_to_terminate)


# Background Thread to Regularly Check Scaling
def start_scaling_monitor():
    while True:
        scale_app_tier()
        time.sleep(30)

# Start Scaling Monitor in a Background Thread
threading.Thread(target=start_scaling_monitor, daemon=True).start()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, threaded=True)