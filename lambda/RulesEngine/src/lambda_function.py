import json
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime, timedelta
import dateutil.parser
from decimal import Decimal
import logging
import requests
import time
from botocore.exceptions import ClientError
import dateutil.tz
from collections import Counter
import statistics

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
current_utc_time = datetime.utcnow().replace(tzinfo=dateutil.tz.UTC)
logger.info(f"Lambda function initialized at {current_utc_time.isoformat()}Z")

# Custom JSON encoder
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj) if obj % 1 else int(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DecimalEncoder, self).default(obj)

# AWS clients
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
sqs = boto3.client('sqs')
s3_client = boto3.client('s3')

# DynamoDB tables
rules_table = dynamodb.Table('Rules')
reports_table = dynamodb.Table('ScienceTeamReports')

# S3 bucket
S3_BUCKET_NAME = 'weather-blogs-2025-science'

# Constants
SNS_TOPIC_ARN = 'arn:aws:sns:ap-south-1:580075786360:weather-alerts'
BATCH_QUEUE_URL = 'https://sqs.ap-south-1.amazonaws.com/580075786360/weather-batch-queue'

# Environment config
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')

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

# Severity thresholds
SEVERITY_THRESHOLDS = {
    'weather': {
        'temperature_c': {'low': 32, 'medium': 38, 'high': 42},
        'humidity_percent': {'low': 70, 'medium': 85, 'high': 95},
        'rainfall_mm': {'low': 5, 'medium': 25, 'high': 60},
        'chance_of_rain_percent': {'low': 50, 'medium': 80, 'high': 95},
        'wind_speed_mps': {'low': 10, 'medium': 18, 'high': 25}
    }
}

# Units for metrics
METRIC_UNITS = {
    'temperature_c': '°C',
    'humidity_percent': '%',
    'wind_speed_mps': 'm/s',
    'wind_direction_deg': '°',
    'rainfall_mm': 'mm',
    'chance_of_rain_percent': '%'
}

# Notification templates
NOTIFICATION_TEMPLATES = {
    'farmer': {
        'en': {
            'high_temperature': "Urgent: {farm_id} temperature at {value}{unit} ({severity}). Irrigate crops now.",
            'heavy_rain': "Alert: Heavy rain ({value}{unit}) at {farm_id} ({severity}). Protect low-lying fields.",
            'delta_temperature': "Warning: Temperature at {farm_id} changed by {delta}{unit} since {time_offset} ({severity}). Adjust irrigation.",
            'sequence_rain_wind': "Alert: Rain ({rainfall_mm}mm) followed by high wind ({wind_speed_mps}m/s) at {farm_id} ({severity}). Secure structures.",
            'default': "Weather alert at {farm_id}: {metric} is {value}{unit} ({severity}). Take necessary action."
        },
        'hi': {
            'high_temperature': "तत्काल: {farm_id} पर तापमान {value}{unit} ({severity})। अभी फसलों को पानी दें।",
            'heavy_rain': "चेतावनी: {farm_id} पर भारी बारिश ({value}{unit}) ({severity})। निचले खेतों की रक्षा करें।",
            'delta_temperature': "चेतावनी: {farm_id} पर तापमान {time_offset} से {delta}{unit} बदल गया ({severity})। सिंचाई समायोजित करें।",
            'sequence_rain_wind': "चेतावनी: {farm_id} पर बारिश ({rainfall_mm}मिमी) और तेज हवा ({wind_speed_mps}मी/से) ({severity})। संरचनाओं को सुरक्षित करें।",
            'default': "{farm_id} पर मौसम चेतावनी: {metric} {value}{unit} है ({severity})। आवश्यक कार्रवाई करें।"
        }
    },
    'operations': {
        'en': {
            'high_temperature': "High temperature ({value}{unit}) at {farm_id} ({severity}). Schedule extra irrigation shifts.",
            'heavy_rain': "Heavy rainfall ({value}{unit}) at {farm_id} ({severity}). Check drainage systems.",
            'delta_temperature': "Temperature shift of {delta}{unit} at {farm_id} since {time_offset} ({severity}). Review operations plan.",
            'sequence_rain_wind': "Rain ({rainfall_mm}mm) and wind ({wind_speed_mps}m/s) at {farm_id} ({severity}). Inspect equipment.",
            'default': "{farm_id} alert: {metric} at {value}{unit} ({severity}). Review operational protocols."
        }
    },
    'science_team': {
        'en': {
            'high_temperature': "High temperature ({value}{unit}) at {farm_id} ({severity}). Analyze crop impact: {data}. Trend: {trend}.",
            'heavy_rain': "Heavy rainfall ({value}{unit}) at {farm_id} ({severity}). Assess soil data: {data}. Trend: {trend}.",
            'delta_temperature': "Temperature change of {delta}{unit} at {farm_id} since {time_offset} ({severity}). Study trends: {data}.",
            'sequence_rain_wind': "Rain ({rainfall_mm}mm) and wind ({wind_speed_mps}m/s) at {farm_id} ({severity}). Review: {data}.",
            'default': "{farm_id} weather event: {metric} at {value}{unit} ({severity}). Data: {data}."
        }
    },
    'management': {
        'en': {
            'high_temperature': "Critical: {farm_id} temperature at {value}{unit} ({severity}). Review resource allocation.",
            'heavy_rain': "Critical: Heavy rain ({value}{unit}) at {farm_id} ({severity}). Assess financial impact.",
            'delta_temperature': "Temperature shift of {delta}{unit} at {farm_id} since {time_offset} ({severity}). Strategic review needed.",
            'sequence_rain_wind': "Rain ({rainfall_mm}mm) and wind ({wind_speed_mps}m/s) at {farm_id} ({severity}). Evaluate risks.",
            'default': "{farm_id} alert: {metric} at {value}{unit} ({severity}). Monitor closely."
        }
    },
    'field': {
        'en': {
            'high_temperature': "Urgent: {farm_id} temperature is {value}{unit} ({severity}). Start irrigation now.",
            'heavy_rain': "Alert: Heavy rain ({value}{unit}) at {farm_id} ({severity}). Secure equipment.",
            'delta_temperature': "Warning: Temperature at {farm_id} has risen by {delta}{unit} since {time_offset} ({severity}). Check crops.",
            'sequence_rain_wind': "Rain ({rainfall_mm}mm) and wind ({wind_speed_mps}m/s) at {farm_id} ({severity}). Protect crops.",
            'default': "{farm_id} weather alert: {metric} is {value}{unit} ({severity}). Follow protocols."
        }
    }
}

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

def validate_rule(rule, for_evaluation=False):
    """Validate a rule based on its type and structure."""
    default_values = {
        'stop_on_match': True,
        'priority': "1",
        'data_type': 'current',
        'conflict_resolution': 'first_match',
        'stakeholder': 'field',
        'rule_name': 'Unnamed Rule',
        'rule_type': 'weather',
        'farm_id': None,
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

    # Apply defaults for evaluation mode
    if for_evaluation:
        for field, default in default_values.items():
            if field not in rule:
                rule[field] = default
                logger.info(f"Applied default {field}={default} for rule {rule.get('rule_id')}")

    required_fields = {
        'rule_id', 'rule_name', 'rule_type', 'data_type', 'stakeholder',
        'conditions', 'actions', 'stop_on_match', 'conflict_resolution', 'priority', 'active'
    }

    missing_fields = required_fields - set(rule.keys())
    if missing_fields and not for_evaluation:
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
        if for_evaluation:
            rule['farm_id'] = None  # Evaluate all farms
            logger.info(f"Invalid farm_id {rule.get('farm_id')} for rule {rule['rule_id']}, defaulting to all farms")
        else:
            logger.error(f"Rule {rule['rule_id']} is invalid: Invalid farm_id {rule['farm_id']}")
            return False, f"Invalid farm_id: {rule['farm_id']}"

    valid_metrics = METRICS_BY_TYPE[rule['rule_type']][rule['data_type']]

    def check_conditions(conditions, depth=0):
        if isinstance(conditions, list):
            results = [check_conditions(cond, depth + 1) for cond in conditions]
            return all(r[0] for r in results), [err for r in results for err in r[1]]

        if not isinstance(conditions, dict):
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

def get_historical_trend(cursor, metric, farm_id, table, time_offset):
    """Fetch historical data for trend analysis."""
    try:
        time_column = 'forecast_for' if 'forecast' in table else 'timestamp'
        cursor.execute(
            f"""
            SELECT AVG({metric}) as avg_value
            FROM {table}
            WHERE farm_id = %s AND {time_column} > NOW() - INTERVAL %s
            """,
            (farm_id, time_offset)
        )
        result = cursor.fetchone()
        logger.debug(f"Historical trend query for {metric} at {farm_id}: {result}")
        return result['avg_value'] if result and result['avg_value'] is not None else None
    except Exception as e:
        logger.error(f"Error fetching trend for {metric}: {str(e)}")
        return None

def determine_severity(rule_type, metric, value, delta=None):
    """Determine severity based on metric, value, and optional delta."""
    thresholds = SEVERITY_THRESHOLDS.get(rule_type, {}).get(metric, {})
    if not thresholds:
        return 'low'
    severity = 'low'
    if isinstance(value, bool):
        severity = 'high' if value == thresholds.get('high', False) else 'low'
    elif isinstance(value, str):
        severity = 'high' if value == thresholds.get('high', '') else 'low'
    elif isinstance(value, (int, float, Decimal)) and value is not None:
        value = float(value)
        if value >= thresholds.get('high', float('inf')):
            severity = 'high'
        elif value >= thresholds.get('medium', float('inf')):
            severity = 'medium'
        elif value >= thresholds.get('low', float('inf')):
            severity = 'low'

    if delta is not None:
        delta_thresholds = {
            'temperature_c': 5,
            'humidity_percent': 20,
            'rainfall_mm': 30
        }
        if metric in delta_thresholds and abs(delta) > delta_thresholds[metric]:
            severity = 'high' if severity == 'medium' else ('medium' if severity == 'low' else severity)

    return severity

def preprocess_data(data):
    """Convert data types for JSON serialization."""
    processed_data = {}
    for key, value in data.items():
        if isinstance(value, datetime):
            processed_data[key] = value.isoformat()
        elif isinstance(value, (Decimal, int, float)):
            processed_data[key] = float(value) if isinstance(value, Decimal) and value % 1 else value
        else:
            processed_data[key] = value
    return processed_data

def compose_message(action, rule, data, farm_id, severity, metric, value, timestamp, cursor, delta=None, time_offset=None):
    """Compose notification message."""
    stakeholder = rule['stakeholder']
    scenario = action.get('scenario', 'default')
    language = rule.get('language', 'en') if stakeholder == 'farmer' else 'en'
    template = NOTIFICATION_TEMPLATES.get(stakeholder, {}).get(language, NOTIFICATION_TEMPLATES[stakeholder]['en']).get(scenario, NOTIFICATION_TEMPLATES[stakeholder]['en']['default'])

    unit = METRIC_UNITS.get(metric, '')
    processed_data = preprocess_data(data)

    trend = "Stable"
    if stakeholder == 'science_team':
        historical_value = get_historical_trend(cursor, metric, farm_id, get_table_name(rule['rule_type'], rule['data_type']), '24 hours')
        if historical_value is not None and value is not None:
            trend = "Rising" if float(value) > historical_value else ("Falling" if float(value) < historical_value else "Stable")

    replacements = {
        '{farm_id}': farm_id,
        '{rule_name}': rule['rule_name'],
        '{severity}': severity.capitalize(),
        '{timestamp}': timestamp.isoformat(),
        '{data}': json.dumps(processed_data, cls=DecimalEncoder),
        '{metric}': metric,
        '{value}': str(value),
        '{unit}': unit,
        '{delta}': str(delta) if delta is not None else '',
        '{time_offset}': time_offset if time_offset else '',
        '{trend}': trend
    }

    if scenario == 'sequence_rain_wind':
        replacements['{rainfall_mm}'] = str(data.get('rainfall_mm', 'N/A'))
        replacements['{wind_speed_mps}'] = str(data.get('wind_speed_mps', 'N/A'))

    message = template
    for placeholder, val in replacements.items():
        message = message.replace(placeholder, val)

    return message

def select_channels(stakeholder, severity, timestamp):
    """Select notification channels."""
    ist = dateutil.tz.gettz('Asia/Kolkata')
    timestamp_ist = timestamp.astimezone(ist)
    current_hour = timestamp_ist.hour
    is_night = 0 <= current_hour < 6

    if stakeholder == 'farmer':
        return ['sms'] if severity == 'high' and not is_night else ['app']
    elif stakeholder == 'operations':
        return ['sms'] if severity == 'high' and not is_night else ['email']
    elif stakeholder == 'science_team':
        return ['email'] if severity == 'high' else []
    elif stakeholder == 'management':
        return ['email', 'api_callback'] if severity in ['medium', 'high'] else ['email']
    elif stakeholder == 'field':
        return ['sms', 'app'] if severity == 'high' and not is_night else ['email', 'app']
    return ['email']

def send_notification(notification_type, message, severity, rule_name, timestamp, endpoint=None, retries=0):
    """Send notification via SNS or API callback."""
    try:
        if notification_type == 'sms':
            response = sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=message,
                MessageAttributes={'NotificationType': {'DataType': 'String', 'StringValue': 'sms'}}
            )
            logger.info(f"Sent SMS notification: {message}, Response: {response['MessageId']}")
        elif notification_type == 'email':
            response = sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=message,
                Subject=f"Weather Alert: {rule_name} ({severity.upper()}) at {timestamp}",
                MessageAttributes={'NotificationType': {'DataType': 'String', 'StringValue': 'email'}}
            )
            logger.info(f"Sent email notification: {message}, Response: {response['MessageId']}")
        elif notification_type == 'api_callback':
            if endpoint:
                response = requests.post(endpoint, json={'message': message, 'severity': severity}, timeout=10)
                response.raise_for_status()
                logger.info(f"Sent API callback to {endpoint}: {message}")
        elif notification_type == 'app':
            logger.info(f"App notification queued: {message}")
        return True
    except Exception as e:
        if retries < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
            return send_notification(notification_type, message, severity, rule_name, timestamp, endpoint, retries + 1)
        logger.error(f"Failed to send {notification_type} notification after {MAX_RETRIES} retries: {str(e)}")
        return False

def queue_batch_notification(farm_id, stakeholder, rule_id, message, severity):
    """Queue batch notification for science team."""
    try:
        sqs.send_message(
            QueueUrl=BATCH_QUEUE_URL,
            MessageBody=json.dumps({
                'farm_id': farm_id,
                'stakeholder': stakeholder,
                'rule_id': rule_id,
                'message': message,
                'severity': severity,
                'timestamp': datetime.utcnow().isoformat()
            })
        )
        logger.info(f"Queued batch notification for {farm_id}/{stakeholder}: {message}")
    except Exception as e:
        logger.error(f"Failed to queue batch notification: {str(e)}")

def store_science_team_report(farm_id, rule_id, message, severity, data, timestamp):
    """Store science team report in DynamoDB and S3."""
    try:
        report_id = f"{rule_id}_{farm_id}_{timestamp.isoformat()}"
        report_data = {
            'report_id': report_id,
            'rule_id': rule_id,
            'farm_id': farm_id,
            'message': message,
            'severity': severity,
            'data': json.dumps(data, cls=DecimalEncoder),
            'timestamp': timestamp.isoformat()
        }
        reports_table.put_item(Item=report_data)
        logger.info(f"Stored report in DynamoDB: {report_id}")

        s3_key = f"reports/{report_id}.json"
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(report_data, cls=DecimalEncoder)
        )
        logger.info(f"Stored report in S3: {s3_key}")
    except Exception as e:
        logger.error(f"Failed to store science team report: {str(e)}")

def get_table_name(rule_type, data_type):
    """Get PostgreSQL table name."""
    return f"{data_type}_{rule_type}"

def evaluate_condition(data, condition, table, farm_id, cursor, rule_id):
    """Evaluate a single condition."""
    try:
        metric = condition.get('metric')
        operator = condition.get('operator')
        value = condition.get('value')
        if isinstance(value, (Decimal, str)):
            try:
                value = float(value)
            except (ValueError, TypeError):
                pass

        time_column = 'forecast_for' if 'forecast' in table else 'timestamp'

        logger.info(f"Evaluating condition for rule {rule_id}: {json.dumps(condition, cls=DecimalEncoder)}")

        if operator == 'TIME_WINDOW':
            threshold = condition['threshold']
            min_duration = condition['temporal']['min_duration']
            window = condition['temporal']['window']
            cursor.execute(
                f"""
                SELECT COUNT(*) FROM {table}
                WHERE {metric} {threshold['operator']} %s
                AND {time_column} > NOW() - INTERVAL %s
                AND farm_id = %s
                """,
                (float(threshold['value']) if isinstance(threshold['value'], (int, float, str, Decimal)) else threshold['value'], window, farm_id)
            )
            result = cursor.fetchone()
            if not result:
                logger.info(f"TIME_WINDOW: No data for {metric} in {table}")
                return False, f"No data for {metric}"
            count = result['count']
            min_count = int(min_duration.split()[0]) if 'hour' in min_duration else int(min_duration.split()[0]) // 60
            logger.debug(f"TIME_WINDOW: {metric} {threshold['operator']} {threshold['value']} for {min_duration} in {window}, count: {count}, required: {min_count}")
            return count >= min_count, f"TIME_WINDOW result: {count} >= {min_count}"

        if operator == 'RATE>':
            interval = condition['temporal']['interval']
            cursor.execute(
                f"""
                SELECT {metric}, {time_column} FROM {table}
                WHERE farm_id = %s AND {time_column} <= NOW()
                ORDER BY {time_column} DESC
                LIMIT 2
                """,
                (farm_id,)
            )
            rows = cursor.fetchall()
            if len(rows) < 2:
                logger.info(f"Rate-of-change for {metric}: Not enough data points")
                return False, f"Not enough data points for {metric}"
            time_diff = (rows[0][time_column] - rows[1][time_column]).total_seconds() / 3600
            value_diff = float(rows[0][metric]) - float(rows[1][metric])
            rate = value_diff / time_diff if time_diff != 0 else 0
            expected_rate = float(value) / (float(interval.split()[0]) / 60 if 'minute' in interval else float(interval.split()[0]))
            logger.debug(f"Rate-of-change for {metric}: {rate} vs expected {expected_rate}")
            return rate > expected_rate, f"Rate: {rate} > {expected_rate}"

        if operator == 'DAY_DIFF>':
            day1 = condition['temporal']['day1']
            day2 = condition['temporal']['day2']
            now = datetime.utcnow()
            if day1 == 'today':
                day1_date = now
            elif day1 == 'tomorrow':
                day1_date = now + timedelta(days=1)
            else:
                day1_date = now + timedelta(days=int(day1.split('_')[1]))

            if day2 == 'today':
                day2_date = now
            elif day2 == 'tomorrow':
                day2_date = now + timedelta(days=1)
            else:
                day2_date = now + timedelta(days=int(day2.split('_')[1]))

            day1_start = day1_date.replace(hour=0, minute=0, second=0, microsecond=0)
            day1_end = day1_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            day2_start = day2_date.replace(hour=0, minute=0, second=0, microsecond=0)
            day2_end = day2_date.replace(hour=23, minute=59, second=59, microsecond=999999)

            cursor.execute(
                f"""
                SELECT AVG({metric}) as avg_value
                FROM {table}
                WHERE farm_id = %s AND {time_column} BETWEEN %s AND %s
                """,
                (farm_id, day1_start, day1_end)
            )
            result = cursor.fetchone()
            if not result or result['avg_value'] is None:
                logger.info(f"No data for {metric} on {day1}")
                return False, f"No data for {metric} on {day1}"
            day1_avg = float(result['avg_value'])

            cursor.execute(
                f"""
                SELECT AVG({metric}) as avg_value
                FROM {table}
                WHERE farm_id = %s AND {time_column} BETWEEN %s AND %s
                """,
                (farm_id, day2_start, day2_end)
            )
            result = cursor.fetchone()
            if not result or result['avg_value'] is None:
                logger.info(f"No data for {metric} on {day2}")
                return False, f"No data for {metric} on {day2}"
            day2_avg = float(result['avg_value'])

            diff = day2_avg - day1_avg
            logger.debug(f"Day diff for {metric}: {day2_avg} - {day1_avg} = {diff} vs threshold {value}")
            return diff > float(value), f"Day diff: {diff} > {value}"

        if operator in {'delta_gt', 'delta_lt'}:
            time_offset = condition['reference']['time_offset']
            cursor.execute(
                f"""
                SELECT {metric} FROM {table}
                WHERE farm_id = %s AND {time_column} <= NOW() - INTERVAL %s
                ORDER BY {time_column} DESC LIMIT 1
                """,
                (farm_id, time_offset)
            )
            ref_result = cursor.fetchone()
            if not ref_result:
                logger.info(f"No reference data for {metric} at offset {time_offset}")
                return False, f"No reference data for {metric} at {time_offset}"
            ref_value = float(ref_result[metric])
            latest_value = float(data.get(metric)) if data.get(metric) is not None else None
            if latest_value is None:
                logger.info(f"No data for {metric} in rule {rule_id}")
                return False, f"No data for {metric}"
            delta = latest_value - ref_value
            logger.debug(f"Delta: {latest_value} - {ref_value} = {delta} vs threshold {value}")
            return delta > float(value) if operator == 'delta_gt' else delta < float(value), f"Delta: {delta} {'>' if operator == 'delta_gt' else '<'} {value}"

        if condition.get('temporal') and operator in {'>', '<', '=', '>=', '<='}:
            duration = condition['temporal']['duration']
            cursor.execute(
                f"""
                SELECT COUNT(*) FROM {table}
                WHERE {metric} {operator} %s
                AND {time_column} > NOW() - INTERVAL %s
                AND farm_id = %s
                """,
                (value, duration, farm_id)
            )
            result = cursor.fetchone()
            if not result:
                logger.info(f"No data for {metric} in {table}")
                return False, f"No data for {metric}"
            count = result['count']
            logger.debug(f"Temporal condition: {metric} {operator} {value} for {duration}, count: {count}")
            return count > 0, f"Temporal condition: count {count} > 0"

        latest_value = float(data.get(metric)) if data.get(metric) is not None else None
        if latest_value is None:
            logger.info(f"No data for {metric} in rule {rule_id}")
            return False, f"No data for {metric}"
        if operator == '>':
            return latest_value > float(value), f"{metric}: {latest_value} > {value}"
        elif operator == '<':
            return latest_value < float(value), f"{metric}: {latest_value} < {value}"
        elif operator == '=':
            return latest_value == float(value), f"{metric}: {latest_value} == {value}"
        elif operator == '>=':
            return latest_value >= float(value), f"{metric}: {latest_value} >= {value}"
        elif operator == '<=':
            return latest_value <= float(value), f"{metric}: {latest_value} <= {value}"
        return False, f"Unsupported operator: {operator}"
    except Exception as e:
        logger.error(f"Error evaluating condition in rule {rule_id}: {str(e)}. Condition: {json.dumps(condition, cls=DecimalEncoder)}")
        return False, f"Error: {str(e)}"

def evaluate_sequence(data, sub_conditions, table, farm_id, cursor, rule_id):
    """Evaluate a SEQUENCE operator."""
    try:
        time_column = 'forecast_for' if 'forecast' in table else 'timestamp'
        last_time = None
        max_interval = None

        for i, cond in enumerate(sub_conditions):
            if i % 2 == 1:
                max_interval = cond['within']
                continue

            cursor.execute(
                f"""
                SELECT {time_column} FROM {table}
                WHERE {cond['metric']} {cond['operator']} %s
                AND farm_id = %s
                AND {time_column} > NOW() - INTERVAL '24 hours'
                ORDER BY {time_column} ASC
                LIMIT 1
                """,
                (float(cond['value']) if isinstance(cond['value'], (int, float, str, Decimal)) else cond['value'], farm_id)
            )
            result = cursor.fetchone()
            if not result:
                logger.info(f"Sequence condition failed in rule {rule_id}: {cond['metric']} {cond['operator']} {cond['value']} not found")
                return False, f"Sequence failed: {cond['metric']} {cond['operator']} {cond['value']} not found"

            current_time = result[time_column]
            if last_time and max_interval:
                time_diff = (current_time - last_time).total_seconds() / 60
                max_minutes = int(max_interval.split()[0])
                if time_diff > max_minutes:
                    logger.info(f"Sequence failed in rule {rule_id}: Time between events {time_diff} minutes > {max_interval}")
                    return False, f"Sequence failed: Time between events {time_diff} minutes > {max_interval}"
            last_time = current_time
        logger.info(f"Sequence condition passed for rule {rule_id}")
        return True, "Sequence passed"
    except Exception as e:
        logger.error(f"Error evaluating sequence in rule {rule_id}: {str(e)}. Sub_conditions: {json.dumps(sub_conditions, cls=DecimalEncoder)}")
        return False, f"Error: {str(e)}"

def evaluate_conditions(data, conditions, table, farm_id, cursor, rule_id):
    """Evaluate conditions recursively."""
    try:
        if isinstance(conditions, list):
            results = [
                evaluate_condition(data, cond, table, farm_id, cursor, rule_id) if isinstance(cond, dict) and 'metric' in cond
                else evaluate_conditions(data, cond, table, farm_id, cursor, rule_id)
                for cond in conditions
            ]
            valid = all(r[0] for r in results)
            details = [r[1] for r in results]
            logger.info(f"List conditions for rule {rule_id}: {valid}, Details: {details}")
            return valid, details

        if not isinstance(conditions, dict):
            logger.error(f"Invalid condition format for rule {rule_id}: {conditions}")
            return False, [f"Invalid condition format: {conditions}"]

        operator = conditions.get('operator')
        sub_conditions = conditions.get('sub_conditions', [])

        if operator == 'AND':
            results = [
                evaluate_condition(data, cond, table, farm_id, cursor, rule_id) if isinstance(cond, dict) and 'metric' in cond
                else evaluate_conditions(data, cond, table, farm_id, cursor, rule_id)
                for cond in sub_conditions
            ]
            valid = all(r[0] for r in results)
            details = [r[1] for r in results]
            logger.info(f"AND condition for rule {rule_id}: {valid}, Details: {details}")
            return valid, details
        elif operator == 'OR':
            results = [
                evaluate_condition(data, cond, table, farm_id, cursor, rule_id) if isinstance(cond, dict) and 'metric' in cond
                else evaluate_conditions(data, cond, table, farm_id, cursor, rule_id)
                for cond in sub_conditions
            ]
            valid = any(r[0] for r in results)
            details = [r[1] for r in results]
            logger.info(f"OR condition for rule {rule_id}: {valid}, Details: {details}")
            return valid, details
        elif operator == 'NOT':
            result = evaluate_conditions(data, sub_conditions[0], table, farm_id, cursor, rule_id)
            logger.info(f"NOT condition for rule {rule_id}: {not result[0]}, Details: {result[1]}")
            return not result[0], [f"NOT: {result[1]}"]
        elif operator == 'SEQUENCE':
            result = evaluate_sequence(data, sub_conditions, table, farm_id, cursor, rule_id)
            logger.info(f"SEQUENCE condition for rule {rule_id}: {result[0]}, Details: {result[1]}")
            return result

        result = evaluate_condition(data, conditions, table, farm_id, cursor, rule_id)
        logger.info(f"Single condition for rule {rule_id}: {result[0]}, Details: {result[1]}")
        return result
    except Exception as e:
        logger.error(f"Error evaluating conditions in rule {rule_id}: {str(e)}. Conditions: {json.dumps(conditions, cls=DecimalEncoder)}")
        return False, [f"Error: {str(e)}"]

def resolve_conflict(rules, triggered_actions):
    """Resolve conflicts among triggered actions."""
    if not triggered_actions:
        return []
    if len(rules) <= 1:
        return triggered_actions

    conflict_strategy = rules[0].get('conflict_resolution', 'first_match')
    logger.info(f"Resolving conflicts with strategy: {conflict_strategy}")

    if conflict_strategy == 'highest_priority':
        max_action = max(
            triggered_actions,
            key=lambda x: int(x['rule'].get('priority', 0))
        )
        logger.info(f"Selected highest priority action: {max_action['rule_id']} (Priority: {max_action['rule'].get('priority')})")
        return [max_action]
    elif conflict_strategy == 'most_severe':
        severity_map = {'low': 1, 'medium': 2, 'high': 3}
        max_action = max(
            triggered_actions,
            key=lambda x: severity_map.get(x['severity'], 0)
        )
        logger.info(f"Selected most severe action: {max_action['rule_id']} (Severity: {max_action['severity']})")
        return [max_action]
    else:
        logger.info(f"Selected first match: {triggered_actions[0]['rule_id']}")
        return [triggered_actions[0]]

def get_all_rule_ids():
    """Fetch all rule IDs from DynamoDB."""
    try:
        response = rules_table.scan(ProjectionExpression='rule_id')
        return sorted([item['rule_id'] for item in response.get('Items', [])])
    except ClientError as e:
        logger.error(f"Error fetching rule IDs: {str(e)}")
        return []

def get_majority_weather_data(cursor, rule, farm_id):
    """Fetch weather data for the day from PostgreSQL (simulating four APIs)."""
    try:
        rule_type = rule['rule_type']
        data_type = rule['data_type']
        table = get_table_name(rule_type, data_type)
        time_column = 'timestamp' if data_type == 'current' else 'forecast_for'
        select_fields = ', '.join(METRICS_BY_TYPE[rule_type][data_type] | {time_column})

        start_time = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = start_time + timedelta(days=1)

        cursor.execute(
            f"""
            SELECT {select_fields}
            FROM {table}
            WHERE farm_id = %s AND {time_column} BETWEEN %s AND %s
            ORDER BY {time_column} DESC
            LIMIT 4
            """,
            (farm_id, start_time, end_time)
        )
        data_list = cursor.fetchall()
        if not data_list:
            logger.info(f"No weather data for {farm_id} in {table}")
            return []

        # Simulate four APIs by using available data or duplicating for demo
        api_data = [dict(data) for data in data_list]
        while len(api_data) < 4:
            api_data.append(api_data[0] if api_data else {})

        # Apply majority rule
        majority_data = []
        for timestamp in set(data[time_column] for data in api_data if time_column in data):
            relevant_data = [data for data in api_data if data.get(time_column) == timestamp]
            if not relevant_data:
                continue

            aggregated = {time_column: timestamp}
            for metric in METRICS_BY_TYPE[rule_type][data_type]:
                values = [data.get(metric) for data in relevant_data if metric in data]
                if not values:
                    continue
                # Convert to float for numeric comparison
                try:
                    values = [float(v) for v in values if v is not None]
                except (ValueError, TypeError):
                    values = [v for v in values if v is not None]
                if not values:
                    continue
                # Majority rule: select value with at least 2 agreements
                counter = Counter(values)
                most_common = counter.most_common(1)
                if most_common and most_common[0][1] >= 2:
                    aggregated[metric] = most_common[0][0]
                else:
                    # No majority: use average for numeric, first for non-numeric
                    aggregated[metric] = statistics.mean(values) if all(isinstance(v, (int, float, Decimal)) for v in values) else values[0]
            majority_data.append(aggregated)

        logger.info(f"Majority weather data for {farm_id}, {table}: {json.dumps(majority_data, cls=DecimalEncoder)}")
        return majority_data
    except Exception as e:
        logger.error(f"Error fetching weather data for {farm_id}: {str(e)}")
        return []

def lambda_handler(event, context):
    """Lambda function handler."""
    conn = None
    cursor = None
    retry_count = 0

    while retry_count < MAX_RETRIES:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                cursor_factory=RealDictCursor,
                connect_timeout=30
            )
            cursor = conn.cursor()
            break
        except Exception as e:
            retry_count += 1
            logger.error(f"Database connection attempt {retry_count}/{MAX_RETRIES} failed: {e}")
            if retry_count == MAX_RETRIES:
                return {
                    'statusCode': 500,
                    'headers': {'Access-Control-Allow-Origin': '*'},
                    'body': json.dumps({'error': 'Database connection failed after retries'}, cls=DecimalEncoder)
                }
            time.sleep(RETRY_DELAY)

    try:
        specific_rule_id = None
        farm_ids = list(VALID_FARM_IDS)

        # Handle API request for specific rule evaluation
        if event.get('httpMethod') == 'POST' and event.get('path') == '/evaluate-rule':
            try:
                body = json.loads(event.get('body', '{}'))
                specific_rule_id = body.get('rule_id')
                if not specific_rule_id:
                    return {
                        'statusCode': 400,
                        'headers': {'Access-Control-Allow-Origin': '*'},
                        'body': json.dumps({'error': 'rule_id is required'}, cls=DecimalEncoder)
                    }
                farm_id = body.get('farm_id')
                if farm_id:
                    if farm_id not in VALID_FARM_IDS:
                        return {
                            'statusCode': 400,
                            'headers': {'Access-Control-Allow-Origin': '*'},
                            'body': json.dumps({'error': f"Invalid farm_id: {farm_id}"}, cls=DecimalEncoder)
                        }
                    farm_ids = [farm_id]
            except json.JSONDecodeError as e:
                return {
                    'statusCode': 400,
                    'headers': {'Access-Control-Allow-Origin': '*'},
                    'body': json.dumps({'error': f"Invalid JSON: {str(e)}"}, cls=DecimalEncoder)
                }

        # Fetch rules
        rules = []
        if specific_rule_id:
            response = rules_table.get_item(Key={'rule_id': specific_rule_id})
            if 'Item' not in response:
                valid_rule_ids = get_all_rule_ids()
                logger.error(f"Rule {specific_rule_id} not found. Valid rule IDs: {valid_rule_ids}")
                return {
                    'statusCode': 400,
                    'headers': {'Access-Control-Allow-Origin': '*'},
                    'body': json.dumps({
                        'error': f"Rule {specific_rule_id} not found",
                        'valid_rule_ids': valid_rule_ids
                    }, cls=DecimalEncoder)
                }
            rules = [response['Item']]
        else:
            response = rules_table.scan()
            rules = response.get('Items', [])
            logger.info(f"Fetched {len(rules)} rules for evaluation")

        results = []
        for rule in rules:
            rule_id = rule['rule_id']
            logger.info(f"Evaluating rule: {json.dumps(rule, cls=DecimalEncoder)}")

            is_valid, validation_error = validate_rule(rule, for_evaluation=True)
            if not is_valid:
                logger.error(f"Rule {rule_id} validation failed: {validation_error}")
                results.append({
                    'rule_id': rule_id,
                    'status': 'Invalid',
                    'error': validation_error,
                    'farm_ids': farm_ids
                })
                continue

            # Determine farms to evaluate
            rule_farm_id = rule.get('farm_id')
            evaluation_farm_ids = [rule_farm_id] if rule_farm_id in VALID_FARM_IDS else farm_ids
            logger.info(f"Evaluating rule {rule_id} for farms: {evaluation_farm_ids}")

            rule_results = []
            for farm_id in evaluation_farm_ids:
                logger.info(f"Evaluating rule {rule_id} for farm {farm_id}")
                rule_type = rule['rule_type']
                data_type = rule['data_type']
                table = get_table_name(rule_type, data_type)
                data_list = get_majority_weather_data(cursor, rule, farm_id)

                if not data_list:
                    logger.info(f"No weather data for {rule_id} in {table} for {farm_id}")
                    rule_results.append({
                        'farm_id': farm_id,
                        'status': 'Not triggered',
                        'error': 'No weather data available',
                        'weather_data': []
                    })
                    continue

                triggered_actions = []
                for data in data_list:
                    timestamp = data.get('timestamp' if data_type == 'current' else 'forecast_for')
                    if timestamp is None:
                        logger.debug(f"No timestamp in data: {data}")
                        continue

                    logger.info(f"Weather data input for {rule_id} at {farm_id}: {json.dumps(data, cls=DecimalEncoder)}")
                    valid, condition_details = evaluate_conditions(data, rule['conditions'], table, farm_id, cursor, rule_id)

                    if valid:
                        severity = 'low'
                        primary_metric = None
                        primary_value = None
                        delta = None
                        time_offset = None

                        # Handle conditions as list or single dict
                        conditions = rule['conditions'] if isinstance(rule['conditions'], list) else [rule['conditions']]
                        for cond in conditions:
                            if isinstance(cond, dict) and 'metric' in cond:
                                if cond.get('operator') in {'delta_gt', 'delta_lt'}:
                                    time_offset = cond['reference'].get('time_offset')
                                    ref_value = get_historical_trend(cursor, cond['metric'], farm_id, table, time_offset)
                                    if ref_value is not None:
                                        delta = float(data[cond['metric']]) - ref_value
                                    cond_severity = determine_severity(rule_type, cond['metric'], data[cond['metric']], delta)
                                    if cond_severity == 'high':
                                        severity = 'high'
                                        primary_metric = cond['metric']
                                        primary_value = data[cond['metric']]
                                        break
                                    elif cond_severity == 'medium' and severity != 'high':
                                        severity = 'medium'
                                        primary_metric = cond['metric']
                                        primary_value = data[cond['metric']]
                                    elif cond_severity == 'low' and severity == 'low':
                                        primary_metric = cond['metric']
                                        primary_value = data[cond['metric']]

                        if not primary_metric and conditions:
                            first = conditions[0]
                            primary_metric = first.get('metric', first.get('sub_conditions', [{}])[0].get('metric') if first.get('sub_conditions') else None)
                            primary_value = data.get(primary_metric) if primary_metric else None

                        if primary_value is None:
                            logger.debug(f"No primary value for {primary_metric} in data: {data}")
                            continue

                        actions_taken = []
                        for action in rule['actions']:
                            message = compose_message(
                                action, rule, data, farm_id, severity, primary_metric, primary_value, timestamp, cursor, delta, time_offset
                            )
                            channels = select_channels(rule['stakeholder'], severity, timestamp)
                            action_info = {
                                'type': action['type'],
                                'message': message,
                                'severity': severity,
                                'channels': channels,
                                'timestamp': timestamp.isoformat()
                            }

                            if rule['stakeholder'] == 'science_team' and severity != 'high':
                                queue_batch_notification(farm_id, rule['stakeholder'], rule_id, message, severity)
                                store_science_team_report(farm_id, rule_id, message, severity, data, timestamp)
                            else:
                                for channel in channels:
                                    endpoint = None
                                    if channel == 'api_callback':
                                        endpoint = action.get('endpoint', None)
                                    success = send_notification(channel, message, severity, rule['rule_name'], timestamp, endpoint)
                                    if not success:
                                        logger.error(f"Failed to send notification for channel {channel} in rule {rule_id}")

                            actions_taken.append(action_info)

                        triggered_actions.append({
                            'rule_id': rule_id,
                            'rule': rule,
                            'severity': severity,
                            'actions': actions_taken
                        })

                        if rule.get('stop_on_match', True):
                            break

                # Resolve conflicts if multiple actions are triggered
                resolved_actions = resolve_conflict([rule], triggered_actions)

                # Prepare farm result
                if triggered_actions:
                    farm_result = {
                        'farm_id': farm_id,
                        'status': 'Triggered',
                        'conditions': condition_details,
                        'weather_data': [preprocess_data(data) for data in data_list],
                        'actions': []
                    }
                    for action in resolved_actions:
                        farm_result['actions'].extend(action['actions'])
                    rule_results.append(farm_result)
                else:
                    rule_results.append({
                        'farm_id': farm_id,
                        'status': 'Not triggered',
                        'conditions': condition_details,
                        'weather_data': [preprocess_data(data) for data in data_list],
                        'actions': []
                    })

            results.append({
                'rule_id': rule_id,
                'status': 'Evaluated',
                'farms': rule_results
            })

        # Commit database changes
        conn.commit()

        # Return response
        return {
            'statusCode': 200,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'results': results}, cls=DecimalEncoder)
        }

    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f"Internal server error: {str(e)}"}, cls=DecimalEncoder)
        }

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("Database connection closed")
