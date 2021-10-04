from fastapi import FastAPI, HTTPException
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
from starlette.requests import Request
from starlette.responses import Response
import redis
# import dask.dataframe as dd
import numpy as np

cache_dataset = {}
AWS_S3_BUCKET = 'upload--data'

app = FastAPI()

r = redis.Redis(host='localhost', port=6379, db=0)

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


async def connect(data_name):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id = 'AKIAROQ3A7BE6D2DCHIM',
        aws_secret_access_key = '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'
    )

    response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key="deag.csv")

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        data =  pd.read_csv(response.get("Body"), header = None)
        data.columns = ['Datetime', 'price']
        data['Datetime'] = pd.to_datetime(data['Datetime'])
        if data_name not in cache_dataset or (not cache_dataset[data_name]):
            cache_dataset[data_name] = data

        return data
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
        raise HTTPException(
            status_code=404,
            detail="Item not found",
        )

async def get_cache(key):
    return r.get(key)


@app.get("/test/cache")
async def set_cache(key, value):
    r.set(key, value)




@app.post("/getdata/{data_name}")
async def get_price_data_test(data_name, from_date, to_date):
    if data_name not in cache_dataset:
        data = await connect(data_name)
        if data is None:
            raise HTTPException(
                status_code=404,
                detail="Item not found",
            )
    else:
        data = cache_dataset[data_name]
        if data is None:
            raise HTTPException(
                status_code=404,
                detail=f"Not in cache_dataset: {cache_dataset}",
            )

    request_data = data.loc[(data['Datetime'] >= from_date) & (data['Datetime'] <= to_date)]
    json_data = request_data.to_json(orient='records')
    return json_data

if __name__ == '__main__':

    uvicorn.run("routes:app", port=8000, host='127.0.0.1', log_level="info", reload = True)