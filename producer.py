import boto3
import csv
import json

# AWS SQS configuration
sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = 'https://sqs.us-east-1.amazonaws.com/591447794615/raghul_task1'


# Read CSV file and send messages to SQS
def send_messages_to_sqs(file_path):
    with open(file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            message_body = json.dumps(row)
            print(f"Sending message: {message_body}")
            sqs.send_message(QueueUrl=queue_url, MessageBody=message_body)


# Example usage
csv_file_path = 'file.csv'
send_messages_to_sqs(csv_file_path)
