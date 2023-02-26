import json
import boto3
from datetime import datetime
import requests
from decimal import Decimal
from botocore.exceptions import ClientError


api_key = 'JazzJT68YcpKyfmrsaG4qgkDuB6sCbqapOo5-NArTho7AoMgkJXDO0-IN6_bxf0w7P8gJpuUmLvmVitztTCedR26KC4AKij_lYRHlhvUkZqWlIslHRXCdkMbSBTkY3Yx'
headers = {'Authorization': 'Bearer {}'.format(api_key)}
api_url = 'https://api.yelp.com/v3/businesses/search'

location = 'Manhattan'
table_name = 'yelp-restaurants'
cuisine_types = ['Chinese','Japanese','Italian','Indian','American']
#cuisine_types = ['Greek','Thai','Mexican']

def request(parameter):
    parameter = parameter or {}
    response = requests.get(api_url, headers=headers, params=parameter) 
    response_json = response.json()
    return response_json


def collect_data(cuisine, offset):
    params = {
        'location': location,
        'offset': offset,
        'limit': 50,
        'term': cuisine + " restaurants",
        'sort_by': 'rating'
    }
    return (request(params), cuisine)



def lambda_handler(event, context):
    
    # business_id is the primary/paritition key
    # note they all have unique attributes
    for cuisine in cuisine_types:
        offset = 0
        while offset < 1000:
            data, cuisine = collect_data(cuisine, offset)
            offset += 50
            insert_data(data['businesses'], cuisine)
    return   
    

def insert_data(data_list, cuisine, db=None, table=table_name):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    # overwrite if the same index is provided
    for data in data_list:
        formatted = {}
        formatted['business_id'] = data['id']
        formatted['name'] = data['name']
        formatted['address'] = data['location']['address1']
        formatted['coordinates'] = data['coordinates']
        formatted['number_of_reviews'] = data['review_count']
        formatted['rating'] = data['rating']
        formatted['zip_code'] = data['location']['zip_code']
        formatted['insertedAtTimestamp'] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        formatted['cuisine'] = cuisine
        item = json.loads(json.dumps(formatted), parse_float=Decimal)
        response = table.put_item(Item=item)
    return 
    
    
    
def lookup_data(key, db=None, table=table_name):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.get_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print(response['Item'])
        return response['Item']
        
        
        
def update_item(key, feature, db=None, table=table_name):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    # change student location
    response = table.update_item(
        Key=key,
        UpdateExpression="set #feature=:f",
        ExpressionAttributeValues={
            ':f': feature
        },
        ExpressionAttributeNames={
            "#feature": "from"
        },
        ReturnValues="UPDATED_NEW"
    )
    print(response)
    return response
    
    
    
def delete_item(key, db=None, table=table_name):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.delete_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print(response)
        return response