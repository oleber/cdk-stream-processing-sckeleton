import boto3
from datetime import datetime
import os
import re
import json
import time

s3 = boto3.resource('s3')
sqs = boto3.resource('sqs')

sqs_queue = sqs.Queue(os.environ.get('SQS_NOTIFY_URL'))

def main(event, context):
    print(event)
    time.sleep(1.5)

    (destination_bucket_name,) = re.search('.*:(.*)', os.environ.get('S3_DESTINATION_ARN')).groups()
    date = datetime.now().strftime("%H%M%S")
    day = datetime.now().strftime("%Y%m%d-%H")

    for index in range(0, 1):
        key = f"{os.environ.get('PREFIX')}/{day}/file.{date}.{index}"

        print(f"Generate s3://{destination_bucket_name}/{key}")
        object = s3.Object(destination_bucket_name, key)
        object.put(Body=b'Here we have some data')

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