from fastapi import FastAPI
import boto3
from boto3.dynamodb.conditions import Key
import uvicorn
from starlette.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import gunicorn
import io
import os
import boto3
import pandas as pd
# import dask.dataframe as dd

cache_dataset = {}

AWS_S3_BUCKET = 'upload--data'
AWS_ACCESS_KEY_ID = 'AKIAROQ3A7BEWWFL3FIN'
AWS_SECRET_ACCESS_KEY = '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'
AWS_SESSION_TOKEN = '0'

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
    allow_credentials=True,
    expose_headers=['content-disposition']
)

class RequestBody(BaseModel):
    from_dtime: str
    to_dtime: str


@app.post("/silver")
def get_price_data(requestbody: RequestBody):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('silver')
    response = table.query(
        KeyConditionExpression = (Key('year').eq(int(2015)) | Key('year').eq(int(2016)) | Key('year').eq(int(2017)) | Key('year').eq(int(2018)) | Key('year').eq(int(2019)) |Key('year').eq(int(2020)))
                                 & Key('dtime').between(requestbody.from_dtime, requestbody.to_dtime)
    )

    return response['Items']

@app.post("/test/silver", tags = ['test'])
def get_price_data(requestbody: RequestBody):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('silver')
    response = table.query(
        KeyConditionExpression = Key('year').eq(int(2016)) & Key('dtime').between(requestbody.from_dtime, requestbody.to_dtime)
    )

    return response['Items']


@app.post("/{data_name}/{from_date}_{to_date}")
async def get_price_data_test(data_name, from_date, to_date):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
    )

    response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key="deag.csv")

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        data = await pd.read_csv(response.get("Body"), header = None)
        data.columns = ['Datetime', 'price']
        data['Datetime'] = pd.to_datetime(data['Datetime'])
        if data_name not in cache_dataset or (not cache_dataset[data_name]):
            cache_dataset[data_name] = data

        request_data = data.loc[(data['Datetime'] >= from_date) & (data['Datetime'] <= to_date)]

        return request_data
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
        return None

if __name__ == '__main__':

    uvicorn.run(app, port=8000, host='127.0.0.1', log_level="info")