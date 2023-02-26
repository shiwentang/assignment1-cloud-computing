import boto3
import json
import random
from boto3.dynamodb.conditions import Key
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

REGION = 'us-east-1'
HOST = 'search-restaurants-nj5mtscp3r6lmgrty3w7u4smme.us-east-1.es.amazonaws.com'
INDEX = 'restaurants'

def lambda_handler(event, context):
    pollSQS()
    

def pollSQS():
    client = boto3.client('sqs')
    queues = client.list_queues(QueueNamePrefix='Q1')
    queue_url = queues['QueueUrls'][0]
    
    while True:
        response = client.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            MaxNumberOfMessages=10,
            MessageAttributeNames=['All'],
            VisibilityTimeout=30,
            WaitTimeSeconds=0
        )
        if 'Messages' in response:
            for message in response['Messages']:
                js = json.loads(message['Body'])
                client.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
                
                cuisine = js['cuisine']
                dining_date = js['dining_date']
                num_people = js['num_people']
                email = js['email']
                dining_time = js['dining_time']
                city = js['city']
                
                # query in opensearch 
                opensearch_results = opensearch_query(cuisine)
                opensearch_results_size_3 = []
                random_index = []
                while len(random_index) < 3:
                    num = random.randint(0,29)
                    if num not in random_index:
                        random_index.append(num)
                for i in random_index:
                    opensearch_results_size_3.append(opensearch_results[i])
                    
                results = ''
                
                count = 1
                for result in opensearch_results_size_3:
                    # query in dynamoDB
                    businessID = result['RestaurantID']
                    restaurant_info = dynamodb_lookup_data({'business_id': businessID})
                    restaurant_info_str = ' '+str(count)+'. '+restaurant_info['name']+', located at '+restaurant_info['address']+','
                    results += restaurant_info_str
                    count += 1

                email_body = "Hello! Here are my "+cuisine+" restaurant suggestions for "+num_people \
                +" people, for "+dining_date+" at "+dining_time+":"+results[:-1]+". Enjoy your meal!"
                send_email(email, email_body)
                print(email_body)
                
        else:
            print('Queue is now empty')
            break
        
def opensearch_query(term):
    q = {'size': 30, 'query': {'multi_match': {'query': term}}}
    client = OpenSearch(hosts=[{
                        'host': HOST,
                        'port': 443}],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)
                        
    res = client.search(index=INDEX, body=q)
    print(res)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])
    return results
    
    
def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)
                    
                    
def dynamodb_lookup_data(key, db=None, table= 'yelp-restaurants'):
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
        
        
def send_email(dest, message):
    ses = boto3.client('ses')
    response = ses.send_email(
        Source = 'coms6998cloudcomputing@gmail.com',
        Destination = {'ToAddresses': [dest]},
        Message = {'Subject':{'Data': 'Restaurant Suggestions From ChatBot-V'},
                    'Body':{'Text':{'Data':message}}}
        )
        