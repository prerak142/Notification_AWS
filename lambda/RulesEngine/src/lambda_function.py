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

# Valid farm IDs from weather fetcher
VALID_FARM_IDS = {'udaipur_farm1', 'location2', 'windy'}

# Valid metrics
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
    'temperature_c': '\u00b0C',
    'humidity_percent': '%',
    'wind_speed_mps': 'm/s',
    'wind_direction_deg': '\u00b0',
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
            'default': "{farm_id} पर मौसम चेतावनी: {metric} {value}{unit} ({severity}) है। आवश्यक कार्रवाई करें।"
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
            'delta_temperature': "Warning: Temperature changed by {delta}{unit} at {farm_id} since {time_offset} ({severity}). Check crops.",
            'sequence_rain_wind': "Rain ({rainfall_mm}mm) and wind ({wind_speed_mps}m/s) at {farm_id} ({severity}). Protect crops.",
            'default': "{farm_id} weather alert: {metric} is {value}{unit} ({severity}). Follow protocols."
        }
    }
}

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

def save_rule(rule):
    """Save a rule to the DynamoDB Rules table."""
    try:
        required_fields = {'rule_id', 'rule_name', 'rule_type', 'farm_scope', 'conditions', 'actions', 'active', 'priority', 'conflict_resolution'}
        missing_fields = required_fields - set(rule.keys())
        if missing_fields:
            logger.error(f"Cannot save rule {rule.get('rule_id', 'unknown')}: Missing fields {missing_fields}")
            return False

        timestamp = datetime.utcnow().isoformat()
        rule['created_at'] = rule.get('created_at', timestamp)
        rule['updated_at'] = timestamp

        farm_scope = rule['farm_scope']
        farm_scope_type = farm_scope['scope_type']
        farm_ids = farm_scope['farm_ids'] if farm_scope_type in ['single', 'multiple'] else ['ALL']

        for farm_id in farm_ids:
            if farm_id != 'ALL' and farm_id not in VALID_FARM_IDS:
                logger.error(f"Invalid farm_id {farm_id} for rule {rule['rule_id']}")
                return False
            item = {
                **rule,
                'farm_id': farm_id,
                'farm_scope_type': farm_scope_type,
                'stakeholder': rule.get('target_role', 'field'),
                'language': rule.get('language', 'en') if rule.get('target_role') == 'farmer' else 'en'
            }
            rules_table.put_item(Item=item)
            logger.info(f"Saved rule {rule['rule_id']} for farm_id {farm_id}")

        return True
    except ClientError as e:
        logger.error(f"Failed to save rule {rule.get('rule_id', 'unknown')}: {str(e)}")
        return False

def validate_rule(rule):
    """Validate a rule based on its type and structure."""
    required_fields = {
        'farm_id', 'stakeholder', 'rule_id', 'rule_name', 'rule_type',
        'data_type', 'conditions', 'actions', 'stop_on_match', 'conflict_resolution', 'priority'
    }
    missing_fields = required_fields - set(rule.keys())
    if missing_fields:
        logger.error(f"Rule {rule['rule_id']} is invalid: Missing fields {missing_fields}")
        return False

    if rule['rule_type'] not in METRICS_BY_TYPE:
        logger.error(f"Rule {rule['rule_id']} is invalid: Invalid rule_type {rule['rule_type']}")
        return False
    if rule['data_type'] not in {'current', 'forecast'}:
        logger.error(f"Rule {rule['rule_id']} is invalid: Invalid data_type {rule['data_type']}")
        return False
    if rule['stakeholder'] not in VALID_STAKEHOLDERS:
        logger.error(f"Rule {rule['rule_id']} is invalid: Invalid stakeholder {rule['stakeholder']}")
        return False
    if rule['conflict_resolution'] not in VALID_CONFLICT_RESOLUTIONS:
        logger.error(f"Rule {rule['rule_id']} is invalid: Invalid conflict_resolution {rule['conflict_resolution']}")
        return False
    if rule.get('language') and rule['stakeholder'] == 'farmer' and rule['language'] not in VALID_LANGUAGES:
        logger.error(f"Rule {rule['rule_id']} is invalid: Invalid language {rule['language']}")
        return False
    if not isinstance(rule['priority'], (int, str)) or (isinstance(rule['priority'], str) and not rule['priority'].isdigit()):
        logger.error(f"Rule {rule['rule_id']} is invalid: Priority must be a number")
        return False

    valid_metrics = METRICS_BY_TYPE[rule['rule_type']][rule['data_type']]

    def check_conditions(conditions, depth=0):
        if isinstance(conditions, list):
            return all(check_conditions(cond, depth + 1) for cond in conditions)

        operator = conditions.get('operator')
        if operator not in VALID_OPERATORS:
            logger.error(f"Rule {rule['rule_id']} is invalid: Invalid operator {operator} at depth {depth}")
            return False

        if operator in {'AND', 'OR', 'NOT', 'SEQUENCE'}:
            sub_conditions = conditions.get('sub_conditions', [])
            if not sub_conditions:
                logger.error(f"Rule {rule['rule_id']} is invalid: No sub_conditions for operator {operator} at depth {depth}")
                return False
            if operator == 'NOT' and len(sub_conditions) != 1:
                logger.error(f"Rule {rule['rule_id']} is invalid: NOT operator requires one sub_condition at depth {depth}")
                return False
            if operator == 'SEQUENCE':
                for i, cond in enumerate(sub_conditions):
                    if i % 2 == 1 and 'within' not in cond:
                        logger.error(f"Rule {rule['rule_id']} is invalid: Expected 'within' in odd-indexed sub_condition for SEQUENCE at depth {depth}")
                        return False
                    elif i % 2 == 0 and 'within' in cond:
                        logger.error(f"Rule {rule['rule_id']} is invalid: Expected condition, not 'within', in even-indexed sub_condition for SEQUENCE at depth {depth}")
                        return False
                    elif 'within' in cond and not cond['within']:
                        logger.error(f"Rule {rule['rule_id']} is invalid: 'within' value missing or empty in SEQUENCE at depth {depth}")
                        return False
            return all(check_conditions(cond, depth + 1) for cond in sub_conditions if 'metric' in cond or 'sub_conditions' in cond)

        metric = conditions.get('metric')
        if not metric and operator != 'SEQUENCE':
            logger.error(f"Rule {rule['rule_id']} is invalid: No metric specified for operator {operator} at depth {depth}")
            return False

        if metric and metric not in valid_metrics:
            logger.error(f"Rule {rule['rule_id']} is invalid: Metric {metric} not available for {rule['rule_type']}/{rule['data_type']} at depth {depth}")
            return False

        if operator in {'>', '<', '=', '>=', '<=', 'delta_gt', 'delta_lt', 'RATE>', 'DAY_DIFF>'}:
            if 'value' not in conditions:
                logger.error(f"Rule {rule['rule_id']} is invalid: No value specified for operator {operator} at depth {depth}")
                return False
        if operator in {'RATE>'}:
            if 'temporal' not in conditions or 'interval' not in conditions['temporal']:
                logger.error(f"Rule {rule['rule_id']} is invalid: RATE> requires temporal.interval at depth {depth}")
                return False
        if operator == 'DAY_DIFF>':
            if 'temporal' not in conditions or 'day1' not in conditions['temporal'] or 'day2' not in conditions['temporal']:
                logger.error(f"Rule {rule['rule_id']} is invalid: DAY_DIFF> requires temporal.day1 and day2 at depth {depth}")
                return False
        if operator in {'delta_gt', 'delta_lt'}:
            if 'reference' not in conditions or 'time_offset' not in conditions['reference']:
                logger.error(f"Rule {rule['rule_id']} is invalid: {operator} requires reference.time_offset at depth {depth}")
                return False
        if operator == 'TIME_WINDOW':
            if 'threshold' not in conditions or 'temporal' not in conditions or 'min_duration' not in conditions['temporal'] or 'window' not in conditions['temporal']:
                logger.error(f"Rule {rule['rule_id']} is invalid: TIME_WINDOW requires threshold and temporal.min_duration/window at depth {depth}")
                return False

        return True

    if not check_conditions(rule['conditions']):
        return False

    for action in rule['actions']:
        if 'type' not in action or action['type'] not in VALID_NOTIFICATION_TYPES:
            logger.error(f"Rule {rule['rule_id']} is invalid: Invalid action type {action.get('type')}")
            return False
        if 'message' not in action or not action['message']:
            logger.error(f"Rule {rule['rule_id']} is invalid: Action missing or empty message")
            return False
        if 'scenario' not in action:
            logger.error(f"Rule {rule['rule_id']} is invalid: Action missing scenario")
            return False

    logger.info(f"Rule {rule['rule_id']} is valid")
    return True

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
    elif value >= thresholds.get('high', float('inf')):
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
            trend = "Rising" if value > historical_value else ("Falling" if value < historical_value else "Stable")

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
    current_hour = timestamp.hour
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
            value = float(value)

        time_column = 'forecast_for' if 'forecast' in table else 'timestamp'

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
                (float(threshold['value']), window, farm_id)
            )
            result = cursor.fetchone()
            if not result:
                logger.info(f"TIME_WINDOW: No data for {metric} in {table}")
                return False
            count = result['count']
            min_count = int(min_duration.split()[0]) if 'hour' in min_duration else int(min_duration.split()[0]) // 60
            logger.debug(f"TIME_WINDOW: {metric} {threshold['operator']} {threshold['value']} for {min_duration} in {window}, count: {count}, required: {min_count}")
            return count >= min_count

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
                return False
            time_diff = (rows[0][time_column] - rows[1][time_column]).total_seconds() / 3600
            value_diff = rows[0][metric] - rows[1][metric]
            rate = value_diff / time_diff if time_diff != 0 else 0
            expected_rate = float(value) / (float(interval.split()[0]) / 60 if 'minute' in interval else float(interval.split()[0]))
            logger.debug(f"Rate-of-change for {metric}: {rate} vs expected {expected_rate}")
            return rate > expected_rate

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
                return False
            day1_avg = result['avg_value']

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
                return False
            day2_avg = result['avg_value']

            diff = day2_avg - day1_avg
            logger.debug(f"Day diff for {metric}: {day2_avg} - {day1_avg} = {diff} vs threshold {value}")
            return diff > float(value)

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
                return False
            ref_value = ref_result[metric]
            latest_value = data.get(metric)
            if latest_value is None:
                logger.info(f"No data for {metric} in rule {rule_id}")
                return False
            delta = latest_value - ref_value
            logger.debug(f"Delta: {latest_value} - {ref_value} = {delta} vs threshold {value}")
            return delta > float(value) if operator == 'delta_gt' else delta < float(value)

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
                return False
            count = result['count']
            logger.debug(f"Temporal condition: {metric} {operator} {value} for {duration}, count: {count}")
            return count > 0

        latest_value = data.get(metric)
        if latest_value is None:
            logger.info(f"No data for {metric} in rule {rule_id}")
            return False
        if operator == '>':
            return latest_value > float(value)
        elif operator == '<':
            return latest_value < float(value)
        elif operator == '=':
            return latest_value == float(value)
        elif operator == '>=':
            return latest_value >= float(value)
        elif operator == '<=':
            return latest_value <= float(value)
        return False
    except Exception as e:
        logger.error(f"Error evaluating condition in rule {rule_id}: {str(e)}. Condition: {json.dumps(condition, cls=DecimalEncoder)}")
        raise

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
                return False

            current_time = result[time_column]
            if last_time and max_interval:
                time_diff = (current_time - last_time).total_seconds() / 60
                max_minutes = int(max_interval.split()[0])
                if time_diff > max_minutes:
                    logger.info(f"Sequence failed in rule {rule_id}: Time between events {time_diff} minutes > {max_interval}")
                    return False
            last_time = current_time
        logger.info(f"Sequence condition passed for rule {rule_id}")
        return True
    except Exception as e:
        logger.error(f"Error evaluating sequence in rule {rule_id}: {str(e)}. Sub_conditions: {json.dumps(sub_conditions, cls=DecimalEncoder)}")
        raise

def evaluate_conditions(data, conditions, table, farm_id, cursor, rule_id):
    """Evaluate conditions recursively."""
    try:
        if isinstance(conditions, list):
            return all(evaluate_condition(data, cond, table, farm_id, cursor, rule_id) for cond in conditions)

        operator = conditions.get('operator')
        sub_conditions = conditions.get('sub_conditions', [])

        if operator == 'AND':
            return all(
                evaluate_condition(data, cond, table, farm_id, cursor, rule_id) if 'metric' in cond
                else evaluate_conditions(data, cond, table, farm_id, cursor, rule_id)
                for cond in sub_conditions
            )
        elif operator == 'OR':
            return any(
                evaluate_condition(data, cond, table, farm_id, cursor, rule_id) if 'metric' in cond
                else evaluate_conditions(data, cond, table, farm_id, cursor, rule_id)
                for cond in sub_conditions
            )
        elif operator == 'NOT':
            return not evaluate_conditions(data, sub_conditions[0], table, farm_id, cursor, rule_id)
        elif operator == 'SEQUENCE':
            return evaluate_sequence(data, sub_conditions, table, farm_id, cursor, rule_id)

        return evaluate_condition(data, conditions, table, farm_id, cursor, rule_id)
    except Exception as e:
        logger.error(f"Error evaluating conditions in rule {rule_id}: {str(e)}. Conditions: {json.dumps(conditions, cls=DecimalEncoder)}")
        raise

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
            key=lambda x: next(int(r['priority']) for r in rules if r['rule_id'] == x['rule_id'])
        )
        logger.info(f"Selected highest priority action: {max_action['rule_id']} (Priority: {next(r['priority'] for r in rules if r['rule_id'] == max_action['rule_id'])})")
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

def batch_notification_processor(event, context):
    """Process batched notifications for science team."""
    try:
        notifications = []
        for record in event['Records']:
            message = json.loads(record['body'])
            notifications.append(message)

        grouped = {}
        for notif in notifications:
            key = f"{notif['farm_id']}_{notif['stakeholder']}"
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(notif)

        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            cursor_factory=RealDictCursor,
            connect_timeout=5
        )
        cursor = conn.cursor()

        for key, notifs in grouped.items():
            farm_id, stakeholder = key.split('_')
            if farm_id not in VALID_FARM_IDS:
                logger.error(f"Invalid farm_id {farm_id} in batch notification")
                continue
            date = datetime.utcnow().strftime('%Y-%m-%d')
            summary = f"Daily Weather Report for {farm_id} on {date}:\n\n"
            blog_content = f"""
Daily Weather Analysis for {farm_id} - {date}

## Overview
Here is a summary of the key weather conditions and alerts triggered based on our monitoring rules.
"""

            cursor.execute(
                """
                SELECT
                    AVG(temperature_c) as avg_temp,
                    MAX(temperature_c) as max_temp,
                    MIN(temperature_c) as min_temp,
                    AVG(humidity_percent) as avg_humidity,
                    AVG(rainfall_mm) as avg_rain,
                    AVG(wind_speed_mps) as avg_wind
                FROM current_weather
                WHERE farm_id = %s AND timestamp >= NOW() - INTERVAL '24 hours'
                """,
                (farm_id,)
            )
            weather_stats = cursor.fetchone() or {}

            blog_content += f"""
### Weather Summary

- **Average Temperature:** {weather_stats.get('avg_temp', 'N/A')} °C
- **Max Temperature:** {weather_stats.get('max_temp', 'N/A')} °C
- **Min Temperature:** {weather_stats.get('min_temp', 'N/A')} °C
- **Humidity:** {weather_stats.get('avg_humidity', 'N/A')}%
- **Rainfall:** {weather_stats.get('avg_rain', 'N/A')} mm
- **Wind Speed:** {weather_stats.get('avg_wind', 'N/A')} m/s

## Triggered Alerts
"""

            for notif in notifs:
                summary += f"- Rule {notif['rule_id']}: {notif['message']} (Severity: {notif['severity']})\n"
                blog_content += f"- **Rule {notif['rule_id']}**: {notif['message']} (Severity: {notif['severity']})\n"

            reports_table.put_item(
                Item={
                    'report_id': f"{farm_id}_{date}",  # Partition key
                    'farm_id': farm_id,
                    'stakeholder': stakeholder,
                    'summary': summary,
                    'timestamp': datetime.utcnow().isoformat()
                }
            )

            s3_key = f"blogs/{farm_id}/{date}.txt"
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=blog_content,
                ContentType='text/plain'
            )
            logger.info(f"Stored blog for {farm_id} on {date} in S3 at {s3_key}")

        cursor.close()
        conn.close()

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Batch notifications processed successfully'}, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in batch_notification_processor: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}, cls=DecimalEncoder)
        }

def lambda_handler(event, context):
    """Lambda function handler."""
    conn = None
    cursor = None
    rule_cache = {}
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
                    'body': json.dumps({'error': 'Database connection failed after retries'}, cls=DecimalEncoder)
                }
            time.sleep(RETRY_DELAY)

    try:
        farm_id = 'udaipur_farm1'  # Default to valid farm_id

        if event.get('httpMethod') == 'POST' and event.get('path') == '/rules':
            rule = json.loads(event.get('body', '{}'))
            if validate_rule(rule):
                if save_rule(rule):
                    return {
                        'statusCode': 201,
                        'body': json.dumps({'message': 'Rule saved'}, cls=DecimalEncoder)
                    }
                return {
                    'statusCode': 500,
                    'body': json.dumps({'error': 'Failed to save rule'}, cls=DecimalEncoder)
                }
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid rule'}, cls=DecimalEncoder)
            }

        if event.get('test_mode'):
            test_results = []
            test_time_range = event.get('test_time_range', '24 hours')
            test_farm_id = event.get('test_farm_id', farm_id)
            stakeholder = event.get('stakeholder', 'field')

            if test_farm_id not in VALID_FARM_IDS:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': f"Invalid test_farm_id {test_farm_id}"}, cls=DecimalEncoder)
                }

            response = rules_table.query(
                IndexName='StakeholderIndex',
                KeyConditionExpression='farm_id = :farm_id AND stakeholder = :stakeholder_id',
                ExpressionAttributeValues={':farm_id': test_farm_id, ':stakeholder_id': stakeholder}
            )
            rules = response.get('Items', [])

            for rule in rules:
                if not validate_rule(rule):
                    continue
                rule_type = rule['rule_type']
                data_type = rule['data_type']
                table = get_table_name(rule_type, data_type)
                time_column = 'timestamp' if data_type == 'current' else 'forecast_for'
                select_fields = ', '.join(METRICS_BY_TYPE[rule_type][data_type] | {time_column})

                cursor.execute(
                    f"""
                    SELECT {select_fields}
                    FROM {table}
                    WHERE farm_id = %s AND {time_column} >= NOW() - INTERVAL %s
                    ORDER BY {time_column} DESC
                    """,
                    (test_farm_id, test_time_range)
                )
                data_list = [cursor.fetchone()] if data_type == 'current' else cursor.fetchall()
                data_list = [d for d in data_list if d]
                logger.debug(f"Test mode data for {test_farm_id}, {data_type}: {data_list}")

                for data in data_list:
                    timestamp = data.get(time_column)
                    if not timestamp:
                        logger.debug(f"No timestamp in data: {data}")
                        continue
                    if evaluate_conditions(data, rule['conditions'], table, test_farm_id, cursor, rule['rule_id']):
                        severity = 'low'
                        primary_metric = None
                        primary_value = None
                        delta = None
                        time_offset = None

                        conditions = rule['conditions'].get('sub_conditions', []) or [rule['conditions']]
                        for cond in conditions:
                            if 'metric' in cond and cond['metric'] in data:
                                if cond.get('operator') in {'delta_gt', 'delta_lt'}:
                                    time_offset = cond['reference'].get('time_offset')
                                    ref_value = get_historical_trend(cursor, cond['metric'], test_farm_id, table, time_offset)
                                    if ref_value is not None:
                                        delta = data[cond['metric']] - ref_value
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

                        if not primary_metric:
                            first = conditions[0]
                            primary_metric = first.get('metric', first.get('sub_conditions', [{}])[0].get('metric'))
                            primary_value = data.get(primary_metric)

                        if not primary_value:
                            logger.debug(f"No primary value for {primary_metric} in data: {data}")
                            continue

                        for action in rule['actions']:
                            message = compose_message(
                                action,
                                rule,
                                data,
                                test_farm_id,
                                severity,
                                primary_metric,
                                primary_value,
                                timestamp,
                                cursor,
                                delta=delta,
                                time_offset=time_offset
                            )
                            test_results.append({
                                'rule_id': rule['rule_id'],
                                'scenario': action.get('scenario', 'default'),
                                'message': message,
                                'severity': severity,
                                'channels': select_channels(rule['stakeholder'], severity, timestamp),
                                'timestamp': timestamp.isoformat()
                            })

            return {
                'statusCode': 200,
                'body': json.dumps({'test_results': test_results}, cls=DecimalEncoder)
            }

        if 'Records' in event:
            for record in event['Records']:
                if record['eventName'] in ['INSERT', 'MODIFY']:
                    rule = record['dynamodb']['NewImage']
                    farm_id = rule['farm_id']['S']
                    stakeholder = rule['stakeholder']['S']
        else:
            farm_id = event.get('farm_id', farm_id)
            stakeholder = event.get('stakeholder', 'field')

        if farm_id not in VALID_FARM_IDS:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': f"Invalid farm_id {farm_id}"}, cls=DecimalEncoder)
            }

        data_by_type = {'weather': {}}
        for rule_type in ['weather']:
            for data_type in ['current', 'forecast']:
                table = get_table_name(rule_type, data_type)
                time_column = 'timestamp' if data_type == 'current' else 'forecast_for'
                select_fields = ', '.join(METRICS_BY_TYPE[rule_type][data_type] | {time_column})
                cursor.execute(
                    f"""
                    SELECT {select_fields}
                    FROM {table}
                    WHERE farm_id = %s AND {time_column} <= NOW()
                    ORDER BY {time_column} DESC
                    LIMIT %s
                    """,
                    (farm_id, 1 if data_type == 'current' else 10)
                )
                result = cursor.fetchone() if data_type == 'current' else cursor.fetchall()
                data_by_type[rule_type][data_type] = [result] if data_type == 'current' and result else result or []
                logger.debug(f"Data for {farm_id}, {data_type}: {data_by_type[rule_type][data_type]}")

        cache_key = f"{farm_id}_{stakeholder}"
        if cache_key not in rule_cache:
            response = rules_table.query(
                IndexName='StakeholderIndex',
                KeyConditionExpression='farm_id = :fid AND stakeholder = :sid',
                ExpressionAttributeValues={':fid': farm_id, ':sid': stakeholder}
            )
            rule_cache[cache_key] = sorted(response.get('Items', []), key=lambda x: int(x.get('priority', 0)))
        rules = rule_cache[cache_key]

        triggered_actions = []
        for rule in rules:
            if not validate_rule(rule):
                continue
            rule_type = rule['rule_type']
            data_type = rule['data_type']
            table = get_table_name(rule_type, data_type)
            data_list = data_by_type.get(rule_type, {}).get(data_type, [])
            if not data_list:
                logger.debug(f"No data for rule {rule['rule_id']} in {table}")
                continue

            for data in data_list:
                timestamp = data.get('timestamp' if data_type == 'current' else 'forecast_for')
                if timestamp is None:
                    logger.debug(f"No timestamp in data: {data}")
                    continue
                if evaluate_conditions(data, rule['conditions'], table, farm_id, cursor, rule['rule_id']):
                    severity = 'low'
                    primary_metric = None
                    primary_value = None
                    delta = None
                    time_offset = None

                    conditions = rule['conditions'].get('sub_conditions', []) or [rule['conditions']]
                    for cond in conditions:
                        if 'metric' in cond and cond['metric'] in data:
                            if cond.get('operator') in {'delta_gt', 'delta_lt'}:
                                time_offset = cond['reference'].get('time_offset')
                                ref_value = get_historical_trend(cursor, cond['metric'], farm_id, table, time_offset)
                                if ref_value is not None:
                                    delta = data[cond['metric']] - ref_value
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

                    if not primary_metric:
                        first = conditions[0]
                        primary_metric = first.get('metric', first.get('sub_conditions', [{}])[0].get('metric'))
                        primary_value = data.get(primary_metric)

                    if primary_value is None:
                        logger.debug(f"No primary value for {primary_metric} in data: {data}")
                        continue

                    for action in rule['actions']:
                        message = compose_message(
                            action,
                            rule,
                            data,
                            farm_id,
                            severity,
                            primary_metric,
                            primary_value,
                            timestamp,
                            cursor,
                            delta,
                            time_offset
                        )
                        triggered_actions.append({
                            'rule_id': rule['rule_id'],
                            'message': message,
                            'severity': severity,
                            'action': action,
                            'timestamp': timestamp,
                            'rule': rule
                        })

        resolved_actions = resolve_conflict(rules, triggered_actions)

        for action_info in resolved_actions:
            action = action_info['action']
            message = action_info['message']
            severity = action_info['severity']
            timestamp = action_info['timestamp']
            rule = action_info['rule']
            channels = select_channels(rule['stakeholder'], severity, timestamp)

            if rule['stakeholder'] == 'science_team' and severity != 'high':
                queue_batch_notification(farm_id, rule['stakeholder'], rule['rule_id'], message, severity)
            else:
                for channel in channels:
                    endpoint = rule.get('callback_url') if channel == 'api_callback' else None
                    send_notification(channel, message, severity, rule['rule_name'], timestamp, endpoint)

            logger.info(f"Processed action for rule {rule['rule_id']}: {message}")

        return {
    'statusCode': 200,
    'body': json.dumps({'message': 'Rules processed successfully'}, cls=DecimalEncoder)
}

    except Exception as e:
        logger.error(f"Lambda handler error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}, cls=DecimalEncoder)
        }
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("Database connection closed")
