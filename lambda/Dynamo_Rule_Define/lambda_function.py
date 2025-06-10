import json
import boto3
from botocore.exceptions import ClientError

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('WeatherRules')

def validate_rule(rule):
    """Validate the rule object for required fields and structure."""
    # Check for historical test action
    if rule.get('action') == 'test_historical':
        return False, "Historical testing is not supported by this endpoint. Use the rule engine Lambda."

    required_fields = ['farm_id', 'rule_id', 'name', 'conditions', 'actions', 'priority', 'stakeholder', 'data_type']
    for field in required_fields:
        if field not in rule:
            return False, f"Missing required field: {field}"
    
    # Validate data_type
    if rule['data_type'] not in ['forecast', 'current']:
        return False, f"Invalid data_type: {rule['data_type']}. Must be 'forecast' or 'current'."
    
    # Validate priority
    try:
        priority = int(rule['priority'])
        if not 1 <= priority <= 10:
            return False, "Priority must be an integer between 1 and 10."
    except (TypeError, ValueError):
        return False, "Priority must be an integer."
    
    # Validate conditions structure
    def validate_conditions(conditions):
        if isinstance(conditions, list):
            for cond in conditions:
                if not validate_conditions(cond):
                    return False
        elif isinstance(conditions, dict):
            if 'metric' in conditions:
                valid_metrics = ['temperature_c', 'humidity_percent', 'wind_speed_mps', 'wind_direction_deg', 'rainfall_mm']
                if conditions['metric'] == 'chance_of_rain_percent' and rule['data_type'] == 'current':
                    return False, "Metric 'chance_of_rain_percent' is only valid for forecast data."
                if conditions['metric'] == 'solar_radiation_wm2' and rule['data_type'] == 'forecast':
                    return False, "Metric 'solar_radiation_wm2' is only valid for current data."
                if conditions['metric'] not in valid_metrics + ['chance_of_rain_percent', 'solar_radiation_wm2']:
                    return False, f"Invalid metric: {conditions['metric']}"
                if conditions['operator'] not in ['>', '<', '=', '>=', '<=', 'RATE>']:
                    return False, f"Invalid operator: {conditions['operator']}"
                if 'value' not in conditions or not isinstance(conditions['value'], (int, float)):
                    return False, "Condition must have a numeric value."
                if 'temporal' in conditions:
                    if 'duration' not in conditions['temporal'] or (conditions['operator'] == 'RATE>' and 'interval' not in conditions['temporal']):
                        return False, "Temporal condition must have duration, and RATE> must have interval."
            elif 'operator' in conditions and 'sub_conditions' in conditions:
                if conditions['operator'] not in ['AND', 'OR', 'NOT', 'SEQUENCE']:
                    return False, f"Invalid group operator: {conditions['operator']}"
                if not validate_conditions(conditions['sub_conditions']):
                    return False
            else:
                return False, "Condition must have metric or operator/sub_conditions."
        else:
            return False, "Conditions must be a list or dict."
        return True, None

    valid, error = validate_conditions(rule['conditions'])
    if not valid:
        return False, error

    # Validate actions
    for action in rule['actions']:
        if 'type' not in action or action['type'] not in ['sms', 'email']:
            return False, "Action must have a valid type (sms or email)."
        if 'message' not in action or not isinstance(action['message'], str):
            return False, "Action must have a string message."
    
    # Validate stop_on_match
    if 'stop_on_match' not in rule or not isinstance(rule['stop_on_match'], bool):
        return False, "stop_on_match must be a boolean."

    return True, None

def lambda_handler(event, context):
    print(f"Event received: {json.dumps(event)}")  # Log the event for debugging

    # Check if event is from API Gateway (has httpMethod)
    if 'httpMethod' in event:
        http_method = event['httpMethod']
        if http_method == 'GET':
            try:
                query_params = event.get('queryStringParameters', {}) or {}
                farm_id = query_params.get('farm_id')
                stakeholder = query_params.get('stakeholder')
                if not farm_id or not stakeholder:
                    return {
                        'statusCode': 400,
                        'body': json.dumps({'error': 'Missing farm_id or stakeholder query parameters'}),
                        'headers': {
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Headers': 'Content-Type',
                            'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
                        }
                    }
                response = table.query(
                    IndexName='StakeholderIndex',
                    KeyConditionExpression='farm_id = :fid AND stakeholder = :stake',
                    ExpressionAttributeValues={
                        ':fid': farm_id,
                        ':stake': stakeholder
                    }
                )
                print(f"GET response: {response['Items']}")
                return {
                    'statusCode': 200,
                    'body': json.dumps(response['Items']),
                    'headers': {
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Headers': 'Content-Type',
                        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
                    }
                }
            except ClientError as e:
                print(f"Error in GET: {str(e)}")
                return {
                    'statusCode': 500,
                    'body': json.dumps({'error': str(e)}),
                    'headers': {
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Headers': 'Content-Type',
                        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
                    }
                }
        elif http_method == 'POST':
            try:
                body = json.loads(event['body'])
                print(f"POST body: {json.dumps(body)}")
                
                # Validate rule
                is_valid, error = validate_rule(body)
                if not is_valid:
                    return {
                        'statusCode': 400,
                        'body': json.dumps({'error': f"Invalid rule: {error}"}),
                        'headers': {
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Headers': 'Content-Type',
                            'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
                        }
                    }
                
                table.put_item(Item=body)
                print("Successfully saved to DynamoDB")
                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': 'Rule saved'}),
                    'headers': {
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Headers': 'Content-Type',
                        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
                    }
                }
            except ClientError as e:
                print(f"Error in POST: {str(e)}")
                return {
                    'statusCode': 500,
                    'body': json.dumps({'error': str(e)}),
                    'headers': {
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Headers': 'Content-Type',
                        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
                    }
                }
        elif http_method == 'OPTIONS':
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
                }
            }
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid method'}),
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
                }
            }
    else:
        # Handle direct invocation (e.g., for testing via AWS CLI)
        print("Direct invocation detected")
        try:
            rule = event
            print(f"Direct invocation body: {json.dumps(rule)}")
            
            # Validate rule
            is_valid, error = validate_rule(rule)
            if not is_valid:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': f"Invalid rule: {error}"})
                }
            
            table.put_item(Item=rule)
            print("Successfully saved to DynamoDB (direct)")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Rule saved (direct invocation)'})
            }
        except ClientError as e:
            print(f"Error in direct invocation: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': str(e)})
            }