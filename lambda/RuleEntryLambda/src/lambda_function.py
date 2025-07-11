import json
import boto3
import logging
from decimal import Decimal
from botocore.exceptions import ClientError

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS DynamoDB client
dynamodb = boto3.resource('dynamodb')
rules_table = dynamodb.Table('Rules')

# Valid farm IDs
VALID_FARM_IDS = {'udaipur_farm1', 'location2', 'windy'}

# Valid metrics based on database tables
METRICS_BY_TYPE = {
    'weather': {
        'current': {'temperature_c', 'humidity_percent', 'wind_speed_mps', 'wind_direction_deg', 'rainfall_mm'},
        'forecast': {'temperature_c', 'humidity_percent', 'wind_speed_mps', 'wind_direction_deg', 'rainfall_mm', 'chance_of_rain_percent'}
    }
}

# Valid operators
VALID_OPERATORS = {'>', '<', '=', '>=', '<=', 'delta_gt', 'delta_lt', 'RATE>', 'DAY_DIFF>', 'TIME_WINDOW', 'AND', 'OR', 'NOT', 'SEQUENCE'}

# Valid notification types
VALID_NOTIFICATION_TYPES = {'email', 'sms', 'api_callback', 'app'}

# Valid stakeholders
VALID_STAKEHOLDERS = {'farmer', 'operations', 'science_team', 'management', 'field'}

# Valid conflict resolutions
VALID_CONFLICT_RESOLUTIONS = {'first_match', 'highest_priority', 'most_severe'}

# Valid languages
VALID_LANGUAGES = {'en', 'hi'}

# Custom JSON encoder
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj) if obj % 1 else int(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DecimalEncoder, self).default(obj)

def validate_rule(rule):
    """Validate a rule based on its type and structure."""
    default_values = {
        'stop_on_match': True,
        'priority': "1",
        'data_type': 'current',
        'conflict_resolution': 'first_match',
        'stakeholder': 'field',
        'rule_name': 'Unnamed Rule',
        'rule_type': 'weather',
        'farm_id': None,  # Optional
        'actions': [],
        'conditions': [],
        'active': True
    }

    # Handle legacy fields
    if 'parameter' in rule:
        if rule['parameter'] == 'temperature':
            rule['metric'] = 'temperature_c'
        else:
            rule['metric'] = rule.pop('parameter')
        logger.info(f"Converted legacy field 'parameter' to 'metric' ({rule['metric']}) for rule {rule.get('rule_id')}")
    if 'target_role' in rule:
        target_role = rule.pop('target_role')
        if target_role == 'field_user':
            rule['stakeholder'] = 'field'
        else:
            rule['stakeholder'] = target_role
        logger.info(f"Converted legacy field 'target_role' ({target_role}) to 'stakeholder' ({rule['stakeholder']}) for rule {rule.get('rule_id')}")
    for action in rule.get('actions', []):
        if 'action_type' in action and 'channel' in action:
            action['type'] = action.pop('channel')
            action.pop('action_type')
            logger.info(f"Converted legacy fields 'action_type' and 'channel' to 'type' for action in rule {rule.get('rule_id')}")
        elif 'action_type' in action:
            action['type'] = action.pop('action_type')
            logger.info(f"Converted legacy field 'action_type' to 'type' for action in rule {rule.get('rule_id')}")
        if 'target_role' in action:
            if action['target_role'] == 'field_user':
                action['stakeholder'] = 'field'
            else:
                action['stakeholder'] = action['target_role']
            action.pop('target_role')
            logger.info(f"Converted action's 'target_role' ({action['stakeholder']}) to 'stakeholder' for rule {rule.get('rule_id')}")
    if 'farm_scope' in rule:
        farm_scope = rule.pop('farm_scope')
        if farm_scope.get('scope_type') == 'single' and farm_scope.get('farm_ids'):
            rule['farm_id'] = farm_scope['farm_ids'][0]
        rule.pop('farm_scope_type', None)
        logger.info(f"Converted legacy 'farm_scope' to 'farm_id' for rule {rule.get('rule_id')}")
    if 'source' in rule:
        rule['data_type'] = rule.pop('source')
        logger.info(f"Converted legacy 'source' to 'data_type' for rule {rule.get('rule_id')}")
    for condition in rule.get('conditions', []):
        if isinstance(condition, dict) and 'parameter' in condition:
            if condition['parameter'] == 'temperature':
                condition['metric'] = 'temperature_c'
            else:
                condition['metric'] = condition.pop('parameter')
            logger.info(f"Converted legacy 'parameter' to 'metric' ({condition['metric']}) in condition for rule {rule.get('rule_id')}")
        if isinstance(condition, dict) and 'source' in condition:
            condition.pop('source')
            logger.info(f"Removed invalid 'source' field in condition for rule {rule.get('rule_id')}")
        if isinstance(condition, dict) and 'reference' in condition and 'parameter' in condition['reference']:
            if condition['reference']['parameter'] == 'temperature':
                condition['reference']['metric'] = 'temperature_c'
            else:
                condition['reference']['metric'] = condition['reference'].pop('parameter')
            logger.info(f"Converted legacy 'parameter' to 'metric' ({condition['reference']['metric']}) in condition reference for rule {rule.get('rule_id')}")

    # Apply defaults
    for field, default in default_values.items():
        if field not in rule:
            rule[field] = default
            logger.info(f"Applied default {field}={default} for rule {rule.get('rule_id')}")

    required_fields = {
        'rule_id', 'rule_name', 'rule_type', 'data_type', 'stakeholder',
        'conditions', 'actions', 'stop_on_match', 'conflict_resolution', 'priority', 'active'
    }

    missing_fields = required_fields - set(rule.keys())
    if missing_fields:
        logger.error(f"Rule {rule.get('rule_id', 'unknown')} is invalid: Missing fields {missing_fields}")
        return False, f"Missing fields: {', '.join(missing_fields)}"

    if rule['rule_type'] not in METRICS_BY_TYPE:
        logger.error(f"Rule {rule['rule_id']} is invalid: Invalid rule_type {rule['rule_type']}")
        return False, f"Invalid rule_type: {rule['rule_type']}"
    if rule['data_type'] not in {'current', 'forecast'}:
        logger.error(f"Rule {rule['rule_id']} is invalid: Invalid data_type {rule['data_type']}")
        return False, f"Invalid data_type: {rule['data_type']}"
    if rule['stakeholder'] not in VALID_STAKEHOLDERS:
        logger.error(f"Rule {rule['rule_id']} is invalid: Invalid stakeholder {rule['stakeholder']}")
        return False, f"Invalid stakeholder: {rule['stakeholder']}"
    if rule['conflict_resolution'] not in VALID_CONFLICT_RESOLUTIONS:
        logger.error(f"Rule {rule['rule_id']} is invalid: Invalid conflict_resolution {rule['conflict_resolution']}")
        return False, f"Invalid conflict_resolution: {rule['conflict_resolution']}"
    if rule.get('language') and rule['stakeholder'] == 'farmer' and rule['language'] not in VALID_LANGUAGES:
        logger.error(f"Rule {rule['rule_id']} is invalid: Invalid language {rule['language']}")
        return False, f"Invalid language: {rule['language']}"
    if not isinstance(rule['priority'], (int, str)) or (isinstance(rule['priority'], str) and not rule['priority'].isdigit()):
        logger.error(f"Rule {rule['rule_id']} is invalid: Priority must be a number")
        return False, "Priority must be a number"
    if rule.get('farm_id') and rule['farm_id'] not in VALID_FARM_IDS:
        logger.error(f"Rule {rule['rule_id']} is invalid: Invalid farm_id {rule['farm_id']}")
        return False, f"Invalid farm_id: {rule['farm_id']}"

    valid_metrics = METRICS_BY_TYPE[rule['rule_type']][rule['data_type']]

    def check_conditions(conditions, depth=0):
        if isinstance(conditions, list):
            results = [check_conditions(cond, depth + 1) for cond in conditions]
            return all(r[0] for r in results), [err for r in results for err in r[1]]

        if not isinstance(condition, dict):
            return False, [f"Invalid condition format at depth {depth}"]

        operator = conditions.get('operator')
        if operator not in VALID_OPERATORS:
            return False, [f"Invalid operator: {operator} at depth {depth}"]

        if operator in {'AND', 'OR', 'NOT', 'SEQUENCE'}:
            sub_conditions = conditions.get('sub_conditions', [])
            if not sub_conditions:
                return False, [f"No sub_conditions for operator {operator} at depth {depth}"]
            if operator == 'NOT' and len(sub_conditions) != 1:
                return False, [f"NOT operator requires one sub_condition at depth {depth}"]
            if operator == 'SEQUENCE':
                for i, cond in enumerate(sub_conditions):
                    if i % 2 == 1 and 'within' not in cond:
                        return False, [f"Expected 'within' in odd-indexed sub_condition for SEQUENCE at depth {depth}"]
                    elif i % 2 == 0 and 'within' in cond:
                        return False, [f"Expected condition, not 'within', in even-indexed sub_condition for SEQUENCE at depth {depth}"]
                    elif 'within' in cond and not cond['within']:
                        return False, [f"'within' value missing or empty in SEQUENCE at depth {depth}"]
            results = [check_conditions(cond, depth + 1) for cond in sub_conditions if 'metric' in cond or 'sub_conditions' in cond or 'within' in cond]
            valid = all(r[0] for r in results)
            errors = [err for r in results for err in r[1]]
            return valid, errors

        metric = conditions.get('metric')
        if not metric and operator != 'SEQUENCE':
            return False, [f"No metric specified for operator {operator} at depth {depth}"]

        if metric and metric not in valid_metrics:
            return False, [f"Metric {metric} not available for {rule['rule_type']}/{rule['data_type']} at depth {depth}"]

        if operator in {'>', '<', '=', '>=', '<=', 'delta_gt', 'delta_lt', 'RATE>', 'DAY_DIFF>'} and 'value' not in conditions:
            return False, [f"No value specified for operator {operator} at depth {depth}"]
        if operator in {'RATE>'} and ('temporal' not in conditions or 'interval' not in conditions['temporal']):
            return False, [f"RATE> requires temporal.interval at depth {depth}"]
        if operator == 'DAY_DIFF>' and ('temporal' not in conditions or 'day1' not in conditions['temporal'] or 'day2' not in conditions['temporal']):
            return False, [f"DAY_DIFF> requires temporal.day1 and day2 at depth {depth}"]
        if operator in {'delta_gt', 'delta_lt'} and ('reference' not in conditions or 'time_offset' not in conditions['reference']):
            return False, [f"{operator} requires reference.time_offset at depth {depth}"]
        if operator == 'TIME_WINDOW' and ('threshold' not in conditions or 'temporal' not in conditions or 'min_duration' not in conditions['temporal'] or 'window' not in conditions['temporal']):
            return False, [f"TIME_WINDOW requires threshold and temporal.min_duration/window at depth {depth}"]

        return True, []

    valid, errors = check_conditions(rule['conditions'])
    if not valid:
        logger.error(f"Rule {rule['rule_id']} is invalid: {errors}")
        return False, f"Condition errors: {', '.join(errors)}"

    for action in rule['actions']:
        if 'type' not in action or action['type'] not in VALID_NOTIFICATION_TYPES:
            logger.error(f"Rule {rule['rule_id']} is invalid: Invalid action type {action.get('type')}")
            return False, f"Invalid action type: {action.get('type')}"
        if 'message' not in action or not action['message']:
            logger.error(f"Rule {rule['rule_id']} is invalid: Action missing or empty message")
            return False, "Action missing or empty message"
        if 'scenario' not in action:
            action['scenario'] = 'default'
            logger.info(f"Set default scenario for action in rule {rule['rule_id']}")

    logger.info(f"Rule {rule['rule_id']} is valid")
    return True, ""

def lambda_handler(event, context):
    """Lambda function handler for storing rules."""
    try:
        if event.get('httpMethod') != 'POST':
            return {
                'statusCode': 405,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'Method not allowed'}, cls=DecimalEncoder)
            }

        body = json.loads(event.get('body', '{}'))
        rule = body.get('rule')
        if not rule:
            rule = body  # Fallback: assume body is the rule itself
        if not rule or 'rule_id' not in rule:
            logger.error("No rule or rule_id provided")
            return {
                'statusCode': 400,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'Rule and rule_id are required'}, cls=DecimalEncoder)
            }

        is_valid, validation_error = validate_rule(rule)
        if not is_valid:
            logger.error(f"Rule {rule['rule_id']} validation failed: {validation_error}")
            return {
                'statusCode': 400,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': validation_error}, cls=DecimalEncoder)
            }

        # Store the rule in DynamoDB
        try:
            rules_table.put_item(Item=rule)
            logger.info(f"Stored rule {rule['rule_id']} successfully")
            return {
                'statusCode': 200,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({
                    'message': 'Rule stored successfully',
                    'rule_id': rule['rule_id']
                }, cls=DecimalEncoder)
            }
        except ClientError as e:
            logger.error(f"Error storing rule {rule['rule_id']}: {str(e)}")
            return {
                'statusCode': 500,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': f"Failed to store rule: {str(e)}"}, cls=DecimalEncoder)
            }

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON: {str(e)}")
        return {
            'statusCode': 400,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f"Invalid JSON: {str(e)}"}, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Lambda handler error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)}, cls=DecimalEncoder)
        }
