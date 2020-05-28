import json
import os
import re
import time
from datetime import datetime

import boto3

s3 = boto3.resource('s3')
sqs = boto3.resource('sqs')

sqs_queue_source = sqs.Queue(os.environ.get('SOURCE_SQS_URL'))

sqs_queue = sqs.Queue(os.environ.get('SQS_NOTIFY_URL'))


def main(event, context):
    print(event)
    time.sleep(1.5)

    (destination_bucket_name,) = re.search('.*:(.*)', os.environ.get('S3_DESTINATION_ARN')).groups()
    date = datetime.now().strftime("%H%M%S")
    day = datetime.now().strftime("%Y%m%d/%H")

    body = ""
    while True:
        messages = sqs_queue_source.receive_messages(
            MaxNumberOfMessages=10,
            WaitTimeSeconds=0
        )

        for message in messages:
            print(f"message: {message.body}")
            body += f"{message.body}\n"
            # Let the queue know that the message is processed
            message.delete()

        if len(messages) == 0:
            break

    for index in range(0, 1):
        key = f"{day}/file.{date}.{index}"

        print(f"Generate s3://{destination_bucket_name}/{key}")
        s3_obj = s3.Object(destination_bucket_name, key)
        s3_obj.put(Body=body)

        sqs_queue.send_message(
            MessageBody=json.dumps({
                "bucket": destination_bucket_name,
                "key": key
            }),
            MessageGroupId="123456789"
        )

    return {
        'statusCode': 200,
        'body': event
    }
