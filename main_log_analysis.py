import boto3
from _env import NEW_S3_OBJECT_NOTIFICATION_QUEUE
from log_analysis import LogAnalyzer, prepare_dataframe
from actions_publisher import publish_actions
import time
import json
import botocore
from botocore.config import Config


config = Config(
   retries={
      'max_attempts': 3
   }
)

sqs = boto3.resource('sqs', config=config)
queue = sqs.Queue(NEW_S3_OBJECT_NOTIFICATION_QUEUE)


if __name__ == '__main__':
    analyzer = LogAnalyzer()
    first_run = True
    while True:
        print('sleeping a bit')
        if first_run:
            first_run = False
        else:
            time.sleep(10)
        print('receive_messages')
        try:
            response = queue.receive_messages(
                WaitTimeSeconds=10,
                MaxNumberOfMessages=1
            )

            message = None
            reconc_actions = None
            if len(response) > 0:
                message = response[0]

            if message is not None:
                receipt_handle = message.receipt_handle
                try:
                    body = json.loads(message.body)
                    records = body['Records']
                    if len(records) > 0:
                        print('getting object name')
                        s3_object_name = records[0]["s3"]["object"]["key"]
                        print('prepare_dataframe')
                        df = prepare_dataframe(s3_object_name)
                        print('analyze_log')
                        reconc_actions = analyzer.analyze_log(df)
                        publish_actions(reconc_actions)
                        print('done')
                    else:
                        print('message has no records')
                    message.delete()

                except EnvironmentError as err:
                    message.change_visibility(
                        VisibilityTimeout=0
                    )
                    print(err)
            else:
                print('no new message')
        except botocore.exceptions.ClientError as err:
            print('Error Message: {}'.format(err.response['Error']['Message']))



