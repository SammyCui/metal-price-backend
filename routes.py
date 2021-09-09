from fastapi import FastAPI
import boto3
from boto3.dynamodb.conditions import Key
import uvicorn
from starlette.middleware.cors import CORSMiddleware
from pydantic import BaseModel
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
    year: int
    from_dtime: str
    to_dtime: str


@app.post("/silver")
def get_price_data(requestbody: RequestBody):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('silver')
    print(type(requestbody.year), type(requestbody.from_dtime))
    response = table.query(
        KeyConditionExpression = Key('year').eq(int(requestbody.year)) & Key('dtime').between(requestbody.from_dtime, requestbody.to_dtime)
    )

    return response['Items']

if __name__ == '__main__':

    uvicorn.run(app, port=8000, host='127.0.0.1', log_level="info")