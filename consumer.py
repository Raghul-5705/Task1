import boto3
import psycopg2
import json

# AWS SQS configuration
sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = 'https://sqs.us-east-1.amazonaws.com/591447794615/raghul_task1'

# PostgreSQL database configuration
db_connection_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': '5705',
    'host': 'localhost',
    'port': '5432',  # Replace with your actual PostgreSQL port
}


# Establish connection to PostgreSQL
def connect_to_db():
    return psycopg2.connect(**db_connection_params)


# Process messages from SQS and insert into PostgreSQL
def process_messages_and_insert():
    while True:
        response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=20)
        messages = response.get('Messages', [])

        for message in messages:
            try:
                # Extract data from the message
                message_body = message['Body']

                # Skip empty or incomplete messages
                if not message_body.strip():
                    print("Empty or incomplete message received. Skipping...")
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
                    continue

                print(f"Received message: {message_body}")

                try:
                    data = json.loads(message_body)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from message: {e}")
                    print(f"Invalid message content: {message_body}")

                    # Delete the invalid message from SQS
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
                    continue

                # Insert into PostgreSQL table
                with connect_to_db() as conn, conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO ev_database (
                            Car_name,
                            Car_name_link,
                            Battery,
                            Efficiency,
                            Fast_charge,
                            Price_DE,
                            Range,
                            Top_speed,
                            Acceleration_0_100
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        data.get('Car_name'),
                        data.get('Car_name_link'),
                        data.get('Battery'),
                        data.get('Efficiency'),
                        data.get('Fast_charge'),
                        data.get('Price_DE'),
                        data.get('Range'),
                        data.get('Top_speed'),
                        data.get('Acceleration_0_100'),
                    ))

                    conn.commit()

                # Delete processed message from SQS
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])

            except Exception as e:
                print(f"Error processing message: {e}")


# Example usage
process_messages_and_insert()
