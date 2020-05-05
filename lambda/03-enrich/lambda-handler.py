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
    # save event to logs
    print(event)
    time.sleep(1.5)

    date = datetime.now().strftime("%H%M%S")

    print(f"Found {len(event['Records'])} records")
    print(f"Will notify queue: {sqs_queue}")

    for record in event['Records']:
        msg_body = json.loads(record['body'])
        copy_source = {
            'Bucket': msg_body['bucket'],
            'Key': msg_body['key']
        }

        new_key = f"{msg_body['key']}.{date}"

        (destination_bucket_name,) = re.search('.*:(.*)', os.environ.get('S3_DESTINATION_ARN')).groups()

        print(f"Copy s3://{copy_source['Bucket']}/{copy_source['Key']} to s3://{destination_bucket_name}/{new_key}")

        bucket = s3.Bucket(destination_bucket_name)
        bucket.copy(copy_source, new_key)

        sqs_queue.send_message(
            MessageBody=json.dumps({
                "bucket": destination_bucket_name,
                "key": new_key
            }),
            MessageGroupId="123456789"
        )

    return {
        'statusCode': 200,
        'body': event
    }