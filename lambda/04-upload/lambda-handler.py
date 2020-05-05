import json
import time

def main(event, context):
    # save event to logs
    print(event)
    time.sleep(1.5)

    print(f"Found {len(event['Records'])} records")

    for record in event['Records']:
        msg_body = json.loads(record['body'])
        print(f"Created {msg_body}")

    return {
        'statusCode': 200,
        'body': event
    }