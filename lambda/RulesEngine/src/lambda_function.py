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

# Setup logging with current UTC time
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
current_utc_time = datetime.utcnow().replace(tzinfo=dateutil.tz.UTC)
logger.info(f"Lambda initialized at {current_utc_time.isoformat()}Z")

# Custom JSON encoder for Decimal and datetime
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj) if obj % 1 else int(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DecimalEncoder, self).default(obj)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
sqs = boto3.client('sqs')
rules_table = dynamodb.Table('Rules')
reports_table = dynamodb.Table('ScienceTeamReports')
SNS_TOPIC_ARN = 'arn:aws:sns:ap-south-1:580075786360:weather-alerts'
BATCH_QUEUE_URL = 'https://sqs.ap-south-1.amazonaws.com/580075786360/weather-batch-queue'

# Database credentials
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASSWORD')

# Valid metrics by rule type
METRICS_BY_TYPE = {
    'weather': {
        'current': {'temperature_c', 'humidity_percent', 'wind_speed_mps', 'wind_direction_deg', 'rainfall_mm'},
        'forecast': {'temperature_c', 'humidity_percent', 'wind_speed_mps', 'wind_direction_deg', 'rainfall_mm', 'chance_of_rain_percent'}
    },
    # Commented out: Other rule types not supported due to missing PostgreSQL tables
    # 'supply': {
    #     'current': {'supply_quantity'},
    #     'forecast': {'predicted_supply_quantity'}
    # },
    # 'equipment': {
    #     'current': {'irrigation_pump_status', 'energy_consumption', 'irrigation_flow_rate'},
    #     'forecast': {'predicted_energy_consumption'}
    # },
    # 'task': {
    #     'current': {'task_completion_status', 'task_due_date', 'task_status'},
    #     'forecast': {'predicted_task_completion'}
    # },
    # 'custom': {
    #     'current': {'pest_count', 'crop_health_index', 'water_ph'},
    #     'forecast': {'predicted_pest_count'}
    # }
}

# Valid operators
VALID_OPERATORS = {'>', '<', '=', '>=', '<=', 'delta_gt', 'delta_lt', 'RATE>', 'DAY_DIFF>', 'TIME_WINDOW', 'AND', 'OR', 'NOT', 'SEQUENCE'}

# Valid notification types and stakeholders
VALID_NOTIFICATION_TYPES = {'email', 'sms', 'api_callback', 'app'}
VALID_STAKEHOLDERS = {'farmer', 'operations', 'science_team', 'management', 'field'}
VALID_CONFLICT_RESOLUTIONS = {'first_match', 'highest_priority', 'most_severe'}

# Severity thresholds by rule type
SEVERITY_THRESHOLDS = {
    'weather': {
        'temperature_c': {'low': 32, 'medium': 38, 'high': 42},
        'humidity_percent': {'low': 70, 'medium': 85, 'high': 95},
        'rainfall_mm': {'low': 5, 'medium': 25, 'high': 60},
        'chance_of_rain_percent': {'low': 50, 'medium': 80, 'high': 95},
        'wind_speed_mps': {'low': 10, 'medium': 18, 'high': 25}
    }
    # Commented out: Thresholds for other rule types not supported
    # 'supply': {
    #     'supply_quantity': {'low': 100, 'medium': 50, 'high': 10},
    #     'predicted_supply_quantity': {'low': 100, 'medium': 50, 'high': 10}
    # },
    # 'equipment': {
    #     'irrigation_pump_status': {'high': False},
    #     'energy_consumption': {'low': 500, 'medium': 1000, 'high': 1500},
    #     'irrigation_flow_rate': {'low': 10, 'medium': 5, 'high': 2},
    #     'predicted_energy_consumption': {'low': 500, 'medium': 1000, 'high': 1500}
    # },
    # 'task': {
    #     'task_completion_status': {'high': False},
    #     'task_status': {'high': 'overdue'},
    #     'predicted_task_completion': {'high': False}
    # },
    # 'custom': {
    #     'pest_count': {'low': 20, 'medium': 50, 'high': 100},
    #     'crop_health_index': {'low': 80, 'medium': 60, 'high': 40},
    #     'water_ph': {'low': 6.5, 'medium': 5.5, 'high': 4.5},
    #     'predicted_pest_count': {'low': 20, 'medium': 50, 'high': 100}
    # }
}

# Units for metrics
METRIC_UNITS = {
    'temperature_c': '°C',
    'humidity_percent': '%',
    'wind_speed_mps': 'm/s',
    'wind_direction_deg': '°',
    'rainfall_mm': 'mm',
    'chance_of_rain_percent': '%'
    # Commented out: Units for other rule types not supported
    # 'supply_quantity': 'units',
    # 'predicted_supply_quantity': 'units',
    # 'irrigation_pump_status': '',
    # 'energy_consumption': 'kWh',
    # 'irrigation_flow_rate': 'L/min',
    # 'predicted_energy_consumption': 'kWh',
    # 'task_completion_status': '',
    # 'task_due_date': '',
    # 'task_status': '',
    # 'predicted_task_completion': '',
    # 'pest_count': 'count',
    # 'crop_health_index': '%',
    # 'water_ph': 'pH',
    # 'predicted_pest_count': 'count'
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
            item = {
                **rule,
                'farm_id': farm_id,
                'farm_scope_type': farm_scope_type,
                'stakeholder': rule.get('target_role', 'field')
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

    logger.info(f"Rule {rule['rule_id']} is valid")
    return True

def determine_severity(rule_type, metric, value):
    """Determine severity based on metric and value."""
    thresholds = SEVERITY_THRESHOLDS.get(rule_type, {}).get(metric, {})
    if not thresholds:
        return 'low'
    if isinstance(value, bool):
        return 'high' if value == thresholds.get('high', False) else 'low'
    if isinstance(value, str):
        return 'high' if value == thresholds.get('high', '') else 'low'
    if value >= thresholds.get('high', float('inf')):
        return 'high'
    elif value >= thresholds.get('medium', float('inf')):
        return 'medium'
    elif value >= thresholds.get('low', float('inf')):
        return 'low'
    return 'low'

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

def compose_message(action, rule, data, farm_id, severity, metric, value, timestamp):
    """Compose notification message with placeholders."""
    template = action['message']
    unit = METRIC_UNITS.get(metric, '')
    processed_data = preprocess_data(data)
    replacements = {
        '{farm_id}': farm_id,
        '{rule_name}': rule['rule_name'],
        '{severity}': severity.capitalize(),
        '{timestamp}': timestamp.isoformat(),
        '{data}': json.dumps(processed_data, cls=DecimalEncoder),
        '{metric}': metric,
        '{value}': str(value),
        '{unit}': unit
    }
    for key, val in processed_data.items():
        replacements[f'{{{key}}}'] = str(val)
    message = template
    for placeholder, val in replacements.items():
        message = message.replace(placeholder, val)
    return message

def select_channels(stakeholder, severity):
    """Select notification channels based on stakeholder and severity."""
    if stakeholder == 'farmer':
        return ['sms'] if severity == 'high' else ['app']
    elif stakeholder == 'operations':
        return ['sms'] if severity == 'high' else ['email']
    elif stakeholder == 'management':
        return ['email', 'api_callback'] if severity in ['medium', 'high'] else ['email']
    elif stakeholder == 'science_team':
        return ['email'] if severity == 'high' else []  # Batched for low/medium
    elif stakeholder == 'field':
        return ['sms', 'app'] if severity == 'high' else ['email', 'app']
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
    """Get PostgreSQL table name based on rule type and data type."""
    return f"{data_type}_{rule_type}"

def evaluate_condition(data, condition, table, farm_id, cursor, rule_id):
    """Evaluate a single condition against data."""
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
            count = cursor.fetchone()['count']
            min_count = int(min_duration.split()[0]) if 'hour' in min_duration else int(min_duration.split()[0]) // 60
            logger.info(f"TIME_WINDOW: {metric} {threshold['operator']} {threshold['value']} for {min_duration} in {window}, count: {count}, required: {min_count}")
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
            logger.info(f"Rate-of-change for {metric}: {rate} vs expected {expected_rate}")
            return rate > expected_rate

        if operator == 'DAY_DIFF>':
            day1 = condition['temporal']['day1']
            day2 = condition['temporal']['day2']
            now = datetime.utcnow()
            day1_date = now if day1 == 'today' else now + timedelta(days=1 if day1 == 'tomorrow' else int(day1.split('_')[1]))
            day2_date = now if day2 == 'today' else now + timedelta(days=1 if day2 == 'tomorrow' else int(day2.split('_')[1]))
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
            day1_avg = cursor.fetchone()['avg_value']
            if day1_avg is None:
                logger.info(f"No data for {metric} on {day1}")
                return False

            cursor.execute(
                f"""
                SELECT AVG({metric}) as avg_value
                FROM {table}
                WHERE farm_id = %s AND {time_column} BETWEEN %s AND %s
                """,
                (farm_id, day2_start, day2_end)
            )
            day2_avg = cursor.fetchone()['avg_value']
            if day2_avg is None:
                logger.info(f"No data for {metric} on {day2}")
                return False

            diff = day2_avg - day1_avg
            logger.info(f"Day diff for {metric}: {day2_avg} - {day1_avg} = {diff} vs threshold {value}")
            return diff > float(value)

        if operator in {'delta_gt', 'delta_lt'}:
            time_offset = condition['reference']['time_offset']
            cursor.execute(
                f"""
                SELECT {metric} FROM {table}
                WHERE farm_id = %s AND {time_column} <= NOW() - INTERVAL %s
                ORDER BY {time_column} DESC
                LIMIT 1
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
                logger.info(f"No latest value for {metric} in rule {rule_id}")
                return False
            delta = latest_value - ref_value
            logger.info(f"Delta for {metric}: {latest_value} - {ref_value} = {delta} vs threshold {value}")
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
            count = cursor.fetchone()['count']
            logger.info(f"Temporal condition: {metric} {operator} {value} for {duration}, count: {count}")
            return count > 0

        latest_value = data.get(metric)
        if latest_value is None:
            logger.info(f"No latest value for {metric} in rule {rule_id}")
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
            if i % 2 == 1:  # Odd indices are 'within' conditions
                max_interval = cond['within']
                continue

            cursor.execute(
                f"""
                SELECT {time_column} FROM {table}
                WHERE {cond['metric']} {cond['operator']} %s
                AND farm_id = %s
                AND {time_column} > NOW() - INTERVAL '1 day'
                ORDER BY {time_column} ASC
                LIMIT 1
                """,
                (float(cond['value']) if isinstance(cond['value'], (Decimal, str)) else cond['value'], farm_id)
            )
            result = cursor.fetchone()
            if not result:
                logger.info(f"Sequence condition failed in rule {rule_id}: {cond['metric']} {cond['operator']} {cond['value']} not found")
                return False

            current_time = result[time_column]
            if last_time and max_interval:
                time_diff = (current_time - last_time).total_seconds() / 60
                max_minutes = float(max_interval.split()[0])
                if time_diff > max_minutes:
                    logger.info(f"Sequence failed in rule {rule_id}: Time between events {time_diff} minutes > {max_minutes} minutes")
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
    if len(rules) == 1:
        return triggered_actions
    conflict_strategy = rules[0].get('conflict_resolution', 'first_match')
    if conflict_strategy == 'highest_priority':
        return [max(triggered_actions, key=lambda x: next(int(r['priority']) for r in rules if r['rule_id'] == x['rule_id']))]
    elif conflict_strategy == 'most_severe':
        return [max(triggered_actions, key=lambda x: {'high': 3, 'medium': 2, 'low': 1}[x['severity']])]
    return [triggered_actions[0]]  # Default to first_match

def lambda_handler(event, context):
    """Main Lambda handler for rule processing and saving."""
    conn = None
    cursor = None
    rule_cache = {}
    retry_count = 0

    while retry_count < MAX_RETRIES:
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                host=DB_HOST,
                port=DB_PORT,
                cursor_factory=RealDictCursor,
                connect_timeout=5
            )
            cursor = conn.cursor()
            break
        except psycopg2.Error as e:
            retry_count += 1
            logger.warning(f"Database connection failed (attempt {retry_count}/{MAX_RETRIES}): {str(e)}")
            if retry_count == MAX_RETRIES:
                logger.error("Max retries reached for database connection")
                return {
                    'statusCode': 500,
                    'body': json.dumps({'error': 'Failed to connect to database after retries'}, cls=DecimalEncoder)
                }
            time.sleep(RETRY_DELAY)

    try:
        farm_id = 'udaipur_farm1'
        stakeholder = 'field'

        # Handle rule saving
        if event.get('httpMethod') == 'POST' and event.get('path') == '/rules':
            body = json.loads(event['body'])
            if validate_rule(body):
                if save_rule(body):
                    return {
                        'statusCode': 200,
                        'body': json.dumps({'message': 'Rule saved successfully'}, cls=DecimalEncoder)
                    }
                else:
                    return {
                        'statusCode': 500,
                        'body': json.dumps({'error': 'Failed to save rule'}, cls=DecimalEncoder)
                    }
            else:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Invalid rule'}, cls=DecimalEncoder)
                }

        # Handle rule evaluation
        if 'Records' in event:
            for record in event['Records']:
                if record['eventName'] in ['INSERT', 'MODIFY']:
                    rule = record['dynamodb']['NewImage']
                    farm_id = rule['farm_id']['S']
                    stakeholder = rule['stakeholder']['S']
        else:
            farm_id = event.get('farm_id', 'udaipur_farm1')
            stakeholder = event.get('stakeholder', 'field')

        # Fetch weather data
        data_by_type = {'weather': {}}
        rule_type = 'weather'
        current_table = get_table_name(rule_type, 'current')
        forecast_table = get_table_name(rule_type, 'forecast')
        current_select_fields = ', '.join(METRICS_BY_TYPE[rule_type]['current'] | {'timestamp'})
        forecast_select_fields = ', '.join(METRICS_BY_TYPE[rule_type]['forecast'] | {'forecast_for'})

        cursor.execute(
            f"""
            SELECT {current_select_fields}
            FROM {current_table}
            WHERE timestamp <= NOW()
            AND farm_id = %s
            ORDER BY timestamp DESC LIMIT 1
            """,
            (farm_id,)
        )
        data_by_type[rule_type]['current'] = cursor.fetchone() or {}
        logger.info(f"Current weather data for {farm_id}: {data_by_type[rule_type]['current']}")

        cursor.execute(
            f"""
            SELECT {forecast_select_fields}
            FROM {forecast_table}
            WHERE forecast_for > NOW()
            AND forecast_for <= NOW() + INTERVAL '48 hours'
            AND farm_id = %s
            ORDER BY forecast_for ASC
            """,
            (farm_id,)
        )
        data_by_type[rule_type]['forecast'] = cursor.fetchall() or []
        logger.info(f"Forecast weather data for {farm_id}: {data_by_type[rule_type]['forecast']}")

        # Commented out: Data fetching for other rule types
        # for rule_type in ['supply', 'equipment', 'task', 'custom']:
        #     logger.info(f"Skipping data fetch for {rule_type}: No PostgreSQL table available")
        #     data_by_type[rule_type] = {'current': {}, 'forecast': []}

        # Query rules with caching
        cache_key = f"{farm_id}_{stakeholder}"
        if cache_key not in rule_cache:
            response = rules_table.query(
                IndexName='StakeholderIndex',
                KeyConditionExpression='farm_id = :fid AND stakeholder = :stake',
                ExpressionAttributeValues={':fid': farm_id, ':stake': stakeholder}
            )
            rule_cache[cache_key] = sorted(response.get('Items', []), key=lambda x: int(x['priority']))
        rules = rule_cache[cache_key]

        # Validate and filter rules
        valid_rules = [rule for rule in rules if validate_rule(rule)]
        for rule in rules:
            logger.info(f"Rule ID: {rule['rule_id']}, Name: {rule['rule_name']}, Type: {rule['rule_type']}, Priority: {rule['priority']}")

        triggered_actions = []
        stop_evaluation = False

        # Evaluate weather rules only
        for rule in valid_rules:
            if stop_evaluation:
                break
            rule_type = rule['rule_type']
            if rule_type != 'weather':
                logger.info(f"Skipping rule {rule['rule_id']} (type: {rule_type}): Only weather rules are evaluated due to missing tables")
                continue
            data_type = rule['data_type']
            table = get_table_name(rule_type, data_type)
            data_list = [data_by_type[rule_type][data_type]] if data_type == 'current' else data_by_type[rule_type][data_type]

            for data in data_list:
                if not data:
                    continue
                timestamp = data.get('timestamp' if data_type == 'current' else 'forecast_for')
                logger.info(f"Evaluating rule {rule['rule_id']} (weather/{data_type}) for {farm_id} at {timestamp}")
                if evaluate_conditions(data, rule['conditions'], table, farm_id, cursor, rule['rule_id']):
                    logger.info(f"Rule {rule['rule_id']} triggered for {farm_id} at {timestamp}")
                    severity = 'low'
                    primary_metric = None
                    primary_value = None
                    conditions = rule['conditions']['sub_conditions'] if 'sub_conditions' in rule['conditions'] else [rule['conditions']]
                    for cond in conditions:
                        if 'metric' in cond and cond['metric'] in data:
                            cond_severity = determine_severity(rule_type, cond['metric'], data[cond['metric']])
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
                        primary_metric = conditions[0]['metric']
                        primary_value = data[primary_metric]

                    selected_channels = select_channels(stakeholder, severity)

                    for action in rule['actions']:
                        message = compose_message(action, rule, data, farm_id, severity, primary_metric, primary_value, timestamp)
                        if stakeholder == 'science_team' and severity != 'high':
                            queue_batch_notification(farm_id, stakeholder, rule['rule_id'], message, severity)
                        else:
                            for channel in selected_channels:
                                if channel == action['type']:
                                    if not send_notification(channel, message, severity, rule['rule_name'], timestamp.isoformat(), action.get('endpoint') if channel == 'api_callback' else None):
                                        logger.warning(f"Notification retry failed for {channel} in rule {rule['rule_id']}")
                        triggered_actions.append({
                            'rule_id': rule['rule_id'],
                            'action': action,
                            'severity': severity,
                            'message': message,
                            'channels': selected_channels,
                            'timestamp': timestamp.isoformat()
                        })
                    if rule.get('stop_on_match', True):
                        logger.info(f"Stopping evaluation for {stakeholder} due to stop_on_match in rule {rule['rule_id']}")
                        stop_evaluation = True
                        break
                else:
                    logger.info(f"Rule {rule['rule_id']} not triggered: Conditions not met at {timestamp}")

        # Resolve conflicts
        final_actions = resolve_conflict(valid_rules, triggered_actions)

        return {
            'statusCode': 200,
            'body': json.dumps(final_actions, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Rules engine error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}, cls=DecimalEncoder)
        }
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

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

        for key, notifs in grouped.items():
            farm_id, stakeholder = key.split('_')
            date = datetime.utcnow().strftime('%Y-%m-%d')
            summary = f"Daily Weather Summary for {farm_id} on {date}:\n"
            for notif in notifs:
                summary += f"- Rule {notif['rule_id']}: {notif['message']} (Severity: {notif['severity']})\n"

            reports_table.put_item(
                Item={
                    'report_id': f"{farm_id}_{date}",
                    'farm_id': farm_id,
                    'stakeholder': stakeholder,
                    'summary': summary,
                    'timestamp': datetime.utcnow().isoformat()
                }
            )

            send_notification(
                'email',
                summary,
                severity='low',
                rule_name=f"Daily Weather Summary for {farm_id}",
                timestamp=datetime.utcnow().isoformat()
            )
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Batch notifications processed'}, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Batch processor error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}, cls=DecimalEncoder)
        }
