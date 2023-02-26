import boto3

# Define the client to interact with Lex
client = boto3.client('lexv2-runtime')

def lambda_handler(event, context):
    
    print("my event", event)
    
    msg_from_user = event['messages'][0]['unstructured']['text']
  
    # Initiate conversation with Lex
    response = client.recognize_text(
            botId='GWOJLZGPAB',
            botAliasId='HEKKZ17Y9U', 
            localeId='en_US',
            sessionId='testuser',
            text=msg_from_user)

    # implement a boilerplate response to all messages
    default_msg = {
                  "messages": [
                    {
                      "type": "unstructured",
                      "unstructured": {
                        "text": "I’m still under development. Please come back later.",
                      }
                    }
                  ]
                }
    resp = {
        'statusCode': 200,
        'messages': default_msg["messages"]
    }
                
    msg_from_lex = response.get('messages', [])
    if msg_from_lex:
      
      print(msg_from_lex)
      
      msgs = []
      
      for msg_idx in range(len(msg_from_lex)):
        msgs.append({"type": "unstructured",
                "unstructured": {
                "text": msg_from_lex[msg_idx]['content'],
                }
              })
              
        
      resp = {
          'statusCode': 200,
          'messages': msgs
      }
    
    print(resp)
    
    return resp