import json
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Rules')

def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])  # Get incoming JSON body
        table.put_item(Item=body)         # Store into DynamoDB

        return {
            'statusCode': 200,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'message': 'Rule stored successfully', 'rule_id': body.get('rule_id')})
        }

    except Exception as e:
        return {
            'statusCode': 400,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }
