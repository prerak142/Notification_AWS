import json
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime, timedelta
import dateutil.parser
from decimal import Decimal

# Custom JSON encoder to handle Decimal types
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj) if obj % 1 else int(obj)
        return super(DecimalEncoder, self).default(obj)

# Initialize DynamoDB and SNS clients
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
rules_table = dynamodb.Table('WeatherRules')

# SNS Topic ARN
SNS_TOPIC_ARN = 'arn:aws:sns:ap-south-1:580075786360:weather-alerts'

# Load database credentials from environment variables
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASSWORD')

# Valid metrics for each table
CURRENT_WEATHER_METRICS = {
    'temperature_c', 'humidity_percent', 'wind_speed_mps', 'wind_direction_deg',
    'rainfall_mm', 'solar_radiation_wm2'
}
FORECAST_WEATHER_METRICS = {
    'temperature_c', 'humidity_percent', 'wind_speed_mps', 'wind_direction_deg',
    'rainfall_mm', 'chance_of_rain_percent'
}
VALID_OPERATORS = {'>', '<', '=', 'RATE>', 'DAY_DIFF>', 'TIME_WINDOW', 'AND', 'OR', 'NOT', 'SEQUENCE'}
VALID_ACTION_TYPES = {'email', 'sms'}

def validate_rule(rule):
    # Check required fields
    required_fields = {'farm_id', 'stakeholder', 'rule_id', 'name', 'priority', 'data_type', 'conditions', 'actions', 'stop_on_match', 'conflict_resolution'}
    missing_fields = required_fields - set(rule.keys())
    if missing_fields:
        print(f"Rule {rule.get('rule_id', 'unknown')} is invalid: Missing fields {missing_fields}")
        return False

    # Validate data_type
    if rule['data_type'] not in {'current', 'forecast'}:
        print(f"Rule {rule['rule_id']} is invalid: Invalid data_type {rule['data_type']}")
        return False

    # Validate metrics based on data_type
    valid_metrics = CURRENT_WEATHER_METRICS if rule['data_type'] == 'current' else FORECAST_WEATHER_METRICS

    def check_conditions(conditions):
        if isinstance(conditions, list):
            return all(check_conditions(cond) for cond in conditions)

        operator = conditions.get('operator')
        if operator not in VALID_OPERATORS:
            print(f"Rule {rule['rule_id']} is invalid: Invalid operator {operator}")
            return False

        if operator in {'AND', 'OR', 'NOT', 'SEQUENCE'}:
            sub_conditions = conditions.get('sub_conditions', [])
            if not sub_conditions:
                print(f"Rule {rule['rule_id']} is invalid: No sub_conditions for operator {operator}")
                return False
            if operator == 'NOT' and len(sub_conditions) != 1:
                print(f"Rule {rule['rule_id']} is invalid: NOT operator requires exactly one sub_condition")
                return False
            return all(check_conditions(cond) for cond in sub_conditions)

        metric = conditions.get('metric')
        if not metric:
            # For SEQUENCE, sub_conditions may include 'within' without a metric
            if 'within' in conditions:
                return True
            print(f"Rule {rule['rule_id']} is invalid: No metric specified")
            return False

        if metric not in valid_metrics:
            print(f"Rule {rule['rule_id']} is invalid: Metric {metric} not available for {rule['data_type']}")
            return False

        if operator in {'>', '<', '=', 'RATE>', 'DAY_DIFF>'}:
            if 'value' not in conditions:
                print(f"Rule {rule['rule_id']} is invalid: No value specified for operator {operator}")
                return False
        if operator == 'RATE>':
            if 'temporal' not in conditions or 'interval' not in conditions['temporal']:
                print(f"Rule {rule['rule_id']} is invalid: RATE> requires temporal.interval")
                return False
        if operator == 'DAY_DIFF>':
            if 'temporal' not in conditions or 'day1' not in conditions['temporal'] or 'day2' not in conditions['temporal']:
                print(f"Rule {rule['rule_id']} is invalid: DAY_DIFF> requires temporal.day1 and day2")
                return False
        if operator == 'TIME_WINDOW':
            if 'threshold' not in conditions or 'temporal' not in conditions or 'min_duration' not in conditions['temporal'] or 'window' not in conditions['temporal']:
                print(f"Rule {rule['rule_id']} is invalid: TIME_WINDOW requires threshold and temporal.min_duration/window")
                return False

        return True

    # Validate conditions
    if not check_conditions(rule['conditions']):
        return False

    # Validate actions
    for action in rule['actions']:
        if 'type' not in action or action['type'] not in VALID_ACTION_TYPES:
            print(f"Rule {rule['rule_id']} is invalid: Invalid action type {action.get('type')}")
            return False
        if 'message' not in action or not action['message']:
            print(f"Rule {rule['rule_id']} is invalid: Action missing or empty message")
            return False

    print(f"Rule {rule['rule_id']} is correct")
    return True

def evaluate_condition(data, condition, table, farm_id, cursor):
    metric = condition.get('metric')
    operator = condition.get('operator')
    value = float(condition.get('value')) if isinstance(condition.get('value'), (Decimal, str)) else condition.get('value')

    time_column = 'forecast_for' if table == 'forecast_weather' else 'timestamp'

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
        # Data is recorded hourly, so 1 data point per hour
        min_count = int(min_duration.split()[0]) if 'hour' in min_duration else int(min_duration.split()[0]) // 60
        print(f"TIME_WINDOW: {metric} {threshold['operator']} {threshold['value']} for {min_duration} in {window}, count: {count}, required: {min_count}")
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
            print(f"Rate-of-change for {metric}: Not enough data points")
            return False
        time_diff = (rows[0][time_column] - rows[1][time_column]).total_seconds() / 3600
        value_diff = rows[0][metric] - rows[1][metric]
        rate = value_diff / time_diff if time_diff != 0 else 0
        expected_rate = value / (float(interval.split()[0]) / 60 if 'minute' in interval else float(interval.split()[0]))
        print(f"Rate-of-change for {metric}: {rate} vs expected {expected_rate}")
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
            print(f"No data for {metric} on {day1}")
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
            print(f"No data for {metric} on {day2}")
            return False

        diff = day2_avg - day1_avg
        print(f"Day diff for {metric}: {day2_avg} - {day1_avg} = {diff} vs threshold {value}")
        return diff > value

    if condition.get('temporal') and operator in ['>', '<', '=']:
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
        print(f"Temporal condition: {metric} {operator} {value} for {duration}, count: {count}")
        return count > 0

    latest_value = data.get(metric)
    if latest_value is None:
        print(f"No latest value for {metric}")
        return False
    if operator == '>':
        return latest_value > value
    elif operator == '<':
        return latest_value < value
    elif operator == '=':
        return latest_value == value
    return False

def evaluate_sequence(data, sub_conditions, table, farm_id, cursor):
    time_column = 'forecast_for' if table == 'forecast_weather' else 'timestamp'
    last_time = None
    max_interval = None

    for i, cond in enumerate(sub_conditions):
        if i > 0 and 'within' in sub_conditions[i-1]:
            max_interval = sub_conditions[i-1]['within']

        if 'metric' not in cond:
            continue  # Skip 'within' conditions

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
            print(f"Sequence condition failed: {cond['metric']} {cond['operator']} {cond['value']} not found")
            return False

        current_time = result[time_column]
        if last_time:
            time_diff = (current_time - last_time).total_seconds() / 60
            if max_interval:
                max_minutes = float(max_interval.split()[0])
                if time_diff > max_minutes:
                    print(f"Sequence failed: Time between events {time_diff} minutes > {max_minutes} minutes")
                    return False
        last_time = current_time
    print("Sequence condition passed")
    return True

def evaluate_conditions(data, conditions, table, farm_id, cursor):
    if isinstance(conditions, list):
        return all(evaluate_condition(data, cond, table, farm_id, cursor) for cond in conditions)

    operator = conditions.get('operator')
    sub_conditions = conditions.get('sub_conditions', [])

    if operator == 'AND':
        return all(
            evaluate_condition(data, cond, table, farm_id, cursor) if 'metric' in cond
            else evaluate_conditions(data, cond, table, farm_id, cursor)
            for cond in sub_conditions
        )
    elif operator == 'OR':
        return any(
            evaluate_condition(data, cond, table, farm_id, cursor) if 'metric' in cond
            else evaluate_conditions(data, cond, table, farm_id, cursor)
            for cond in sub_conditions
        )
    elif operator == 'NOT':
        return not evaluate_conditions(data, sub_conditions[0], table, farm_id, cursor)
    elif operator == 'SEQUENCE':
        return evaluate_sequence(data, sub_conditions, table, farm_id, cursor)
    return evaluate_condition(data, conditions, table, farm_id, cursor)

def lambda_handler(event, context):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT,
            cursor_factory=RealDictCursor
        )
        cursor = conn.cursor()

        farm_id = 'udaipur_farm1'
        stakeholder = 'field'
        data_type = 'forecast'

        if 'Records' in event:
            for record in event['Records']:
                if record['eventName'] in ['INSERT', 'MODIFY']:
                    rule = record['dynamodb']['NewImage']
                    farm_id = rule['farm_id']['S']
                    stakeholder = rule['stakeholder']['S']
                    data_type = rule['data_type']['S']
        else:
            farm_id = event.get('farm_id', 'udaipur_farm1')
            stakeholder = event.get('stakeholder', 'field')
            data_type = event.get('data_type', 'forecast')

        table = 'forecast_weather' if data_type == 'forecast' else 'current_weather'
        time_column = 'forecast_for' if data_type == 'forecast' else 'timestamp'

        # Adjust selected fields based on the table
        select_fields = (
            "temperature_c, humidity_percent, wind_speed_mps, wind_direction_deg, rainfall_mm"
            + (", chance_of_rain_percent" if table == 'forecast_weather' else "")
        )

        cursor.execute(
            f"""
            SELECT {select_fields}
            FROM {table}
            WHERE {time_column} > NOW() - INTERVAL '1 day'
            AND farm_id = %s
            ORDER BY {time_column} DESC LIMIT 1
            """,
            (farm_id,)
        )
        data = cursor.fetchone() or {}
        print(f"Latest weather data: {data}")

        # Query using the StakeholderIndex GSI
        response = rules_table.query(
            IndexName='StakeholderIndex',
            KeyConditionExpression='farm_id = :fid AND stakeholder = :stake',
            ExpressionAttributeValues={':fid': farm_id, ':stake': stakeholder}
        )
        rules = sorted(response['Items'], key=lambda x: int(x['priority']))

        # Validate and print all rules
        print("Validating all rules in WeatherRules table:")
        valid_rules = []
        for rule in rules:
            if validate_rule(rule):
                valid_rules.append(rule)
            print(f"Rule ID: {rule['rule_id']}, Name: {rule['name']}, Priority: {rule['priority']}, Conditions: {json.dumps(rule['conditions'], cls=DecimalEncoder)}")

        triggered_actions = []
        for rule in valid_rules:
            if rule['data_type'] != data_type:
                print(f"Rule {rule['rule_id']} skipped: Data type mismatch (expected {data_type}, got {rule['data_type']})")
                continue
            conditions = rule.get('conditions', [])
            if evaluate_conditions(data, conditions, table, farm_id, cursor):
                print(f"Rule {rule['rule_id']} triggered")
                actions = rule['actions']
                for action in actions:
                    if action['type'] == 'email':
                        message = action['message']
                        subject = f"Weather Alert: Rule {rule['name']} Triggered for {farm_id}"
                        try:
                            sns_response = sns.publish(
                                TopicArn=SNS_TOPIC_ARN,
                                Message=message,
                                Subject=subject
                            )
                            print(f"SNS email sent: {sns_response}")
                        except Exception as e:
                            print(f"Error sending SNS email: {str(e)}")
                    elif action['type'] == 'sms':
                        print(f"SMS action triggered: {action['message']}")
                triggered_actions.append({
                    'rule_id': rule['rule_id'],
                    'actions': rule['actions']
                })
                if rule.get('stop_on_match', True):
                    print("Stopping evaluation due to stop_on_match")
                    break
            else:
                print(f"Rule {rule['rule_id']} not triggered: Conditions not met")

        return {
            'statusCode': 200,
            'body': json.dumps(triggered_actions, cls=DecimalEncoder)
        }
    except Exception as e:
        print(f"[ERROR] Rules engine: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}, cls=DecimalEncoder)
        }
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
