import boto3
from botocore.config import Config
from stream_tester_env import STREAM_NAME
import botocore
from datetime import datetime
import uuid
import random
import time
import json


class EventTypes:
    PAY_REQ = 'PAY_REQ'
    PAY_SUC = 'PAY_SUC'
    PAY_FAIL = 'PAY_FAIL'
    PAY_IN_DB = 'PAY_IN_DB'
    PAY_DEL = 'PAY_DEL'
    TIC_REQ = 'TIC_REQ'
    TIC_SUC = 'TIC_SUC'
    TIC_FAIL = 'TIC_FAIL'
    TIC_IN_DB = 'TIC_IN_DB'
    TIC_DEL = 'TIC_DEL'


PROPER_EVENTS_LIST = [EventTypes.PAY_REQ,
                      EventTypes.PAY_SUC,
                      EventTypes.PAY_IN_DB,
                      EventTypes.TIC_REQ,
                      EventTypes.TIC_SUC,
                      EventTypes.TIC_IN_DB]





config = Config(
   retries={
      'max_attempts': 3
       # ,
      # 'mode': 'standard'
   }
)

client = boto3.client('kinesis', config=config)

def publish_to_stream(_events):
    try:
        response = client.put_records(
            Records=[
                {
                    "Data": bytes(
                        json.dumps({
                            'reservation_id': an_event['reservation_id'],
                            'time': an_event['time'],
                            'event_type': an_event['event_type']
                        }), 'utf-8'
                        ),
                    "PartitionKey": str(an_event["geo_id"])
                } for an_event in _events],
            StreamName=STREAM_NAME
        )
        failed_count = response['FailedRecordCount']
        if failed_count > 0:
            print('to do: handle {} failed records'.format(failed_count))
        print('done')
    except botocore.exceptions.ClientError as err:
        print('Error Message: {}'.format(err.response['Error']['Message']))


if __name__ == '__main__':
    while True:
        sleep_time = random.randint(1,5)
        time.sleep(sleep_time)
        _events = []
        for event_type in PROPER_EVENTS_LIST:
            reservation_id = str(uuid.uuid4())[0:7]
            _time = datetime.now()
            _events.append(
                {
                    'geo_id': 1,
                    'reservation_id': reservation_id,
                    'time': datetime.strftime(_time, "%Y-%m-%d %H:%M:%S"),
                    'event_type': event_type
                }
            )
        publish_to_stream(_events)





