import json
import datetime
import time
import os
import dateutil.parser
import logging
import math
import boto3


# modified from the blueprint of order flowers

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

os.putenv('TZ', 'US/Eastern')
time.tzset()


def elicit_slot(session_attributes, intent, slot_to_elicit, message):
    return {'sessionState':
                {'sessionAttributes': session_attributes,
                    'dialogAction': {
                        'type': 'ElicitSlot',
                        'slotToElicit': slot_to_elicit,
                    },
                    'intent':intent,
                },
            'messages': [message],
            }
    
    
def close(session_attributes, fulfillment_state, intent, message):
    response = {'sessionState':
                    {'sessionAttributes': session_attributes,
                        'dialogAction': {
                            'type': 'Close',
                        },
                        'intent': intent,
                    },
                    'messages': [message],
                }
    return response


def delegate(session_attributes, intent):
    return {'sessionState':
                {'sessionAttributes': session_attributes,
                    'dialogAction': {
                        'type': 'Delegate',
                    },
                    'intent':intent,
                }, 
            }

def get_slots(intent_request):
    return intent_request['interpretations'][0]['intent']['slots']
    
def validate_slot(slotname, intent_request):
    if get_slots(intent_request)[slotname] is None:
        return None
    elif len(get_slots(intent_request)[slotname]['value']) == 2:
        if len(get_slots(intent_request)[slotname]['value']['resolvedValues']) == 2:
            return 'TimeFormatError'
    else:
        return get_slots(intent_request)[slotname]['value']['interpretedValue']
        
    
def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }
    
def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False
        
def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')
        
        
def isvalid_num_people(num_people):
    if int(num_people) <= 0:
        return False
    else:
        return True

    
def validate_dining_suggestion(intent_request, cuisine, dining_date, num_people, email, dining_time, city):
    city_types = ["manhattan"]
    if city is not None:
        if city.lower() not in city_types:
            return build_validation_result(False, 'Location', 'Sorry, we cannot suggest restaurants in {}, could you please provide another city?'.format(city))

    cuisine_types = ['chinese','japanese','italian','indian','american', 'greek','thai','mexican']

    if cuisine is not None:
        if cuisine.lower() not in cuisine_types:
            return build_validation_result(False, 'Cuisine', 'Sorry, we cannot suggest {} food restaurant, could you please select one below? (Chinese, Japanese, Italian, Indian, American, Greek, Thai, Mexican)'.format(cuisine))
        
    if dining_date is not None:
        if not isvalid_date(dining_date):
            return build_validation_result(False, 'DiningDate', 'I did not understand that, what date would you like to make a reservation?')
        # reserve for a date in the past
        elif datetime.datetime.strptime(dining_date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'DiningDate', 'You can make a reservation for a date in the future. What date would you like to reserve?')

    if dining_time is not None:
        if dining_time == 'TimeFormatError':
            return build_validation_result(False, 'DiningTime', "Sorry, I cannot understand that. Please type in the format XX:XX am/pm")
        hour, minute = dining_time.split(":")
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            return build_validation_result(False, 'DiningTime', "Please provide a specific time including both hour and minute.")
            
        date_for_dining = validate_slot('DiningDate', intent_request)
        if datetime.datetime.strptime(date_for_dining, '%Y-%m-%d').date() == datetime.date.today():
            if datetime.datetime.strptime(dining_time, '%H:%M').time() < datetime.datetime.now().time():
                return build_validation_result(False, 'DiningTime', 'You can make a reservation for a time in the future. What time would you like to reserve?')
        
            
    if num_people is not None:
        if not isvalid_num_people(num_people):
            return build_validation_result(False, 'NumPeople', 'I did not understand that. How many people are in your party?')


    return build_validation_result(True, None, None)
        
def dining_suggestions_intent(intent_request):
    
    cuisine = validate_slot('Cuisine', intent_request)
    dining_date = validate_slot('DiningDate', intent_request)
    num_people = validate_slot('NumPeople', intent_request)
    email = validate_slot('Email', intent_request)
    dining_time = validate_slot('DiningTime', intent_request)
    city = validate_slot('Location', intent_request)
    source = intent_request['invocationSource']
    
    slots = intent_request['sessionState']['intent']['slots']

    
    if 'proposedNextState' not in intent_request:
        logger.debug('source={}'.format(source))
    
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName='Q1')
        msg = {"cuisine": cuisine, "dining_date": dining_date, "num_people":num_people,
        "email":email, "dining_time":dining_time, "city":city}
        response = queue.send_message(MessageBody=json.dumps(msg))
        
        return close(intent_request['sessionState']['sessionAttributes'], \
                'Fulfilled', \
                intent_request['sessionState']['intent'], \
                {'contentType': 'PlainText',
                  'content': 'Thanks! You will receive message later!'})
        
    
    if source == 'DialogCodeHook':
        slots = get_slots(intent_request)
        
        validation_result = validate_dining_suggestion(intent_request, cuisine, dining_date, num_people, email, dining_time, city)
        
        if not validation_result['isValid']:
             
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionState']['sessionAttributes'],
                               intent_request['sessionState']['intent'],
                               validation_result['violatedSlot'],
                               validation_result['message'])

        if intent_request['sessionState']['sessionAttributes'] is None:
            output_session_attributes = {}
        else:
            output_session_attributes = intent_request['sessionState']['sessionAttributes'] 
            
        return delegate(output_session_attributes, intent_request['sessionState']['intent'])

        

def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch intentName={}'.format(intent_request['sessionState']['intent']['name']))

    intent_name = intent_request['sessionState']['intent']['name']
    
    print("intent request", intent_request)

    if intent_name == 'DiningSuggestionsIntent':
        return dining_suggestions_intent(intent_request)

        
    raise Exception('Intent with name ' + intent_name + ' not supported')




def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    
    print(event)
    
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)