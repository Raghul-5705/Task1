from upload_csv import upload_csv_file
from producer import send_messages_to_sqs
from consumer import process_messages_and_insert

if __name__ == "__main__":
    # Upload CSV file to PostgreSQL database
    #upload_csv_file()

    # Produce messages to the SQS queue
    send_messages_to_sqs('file.csv')

    # Consume messages from the SQS queue and process them
    process_messages_and_insert()