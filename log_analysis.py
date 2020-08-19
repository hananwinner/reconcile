import boto3
import pandas as pd
import s3fs
from _env import BUCKET_NAME


def prepare_dataframe(s3_object_name):
    df = pd.read_parquet('s3://{}/{}'.format(BUCKET_NAME, s3_object_name))
    return df


class LogAnalyzer(object):
    def analyze_log(self, df):
        print(df.head())
