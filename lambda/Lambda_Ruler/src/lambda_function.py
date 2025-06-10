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

def evaluate_condition(data, condition, table, farm_id, cursor):
    metric = condition.get('metric')
    operator = condition.get('operator')
    value = float(condition.get('value')) if isinstance(condition.get('value'), (Decimal, str)) else condition.get('value')

    time_column = 'forecast_for' if table == 'forecast_weather' else 'timestamp'

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
    return False

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

        cursor.execute(
            f"""
            SELECT temperature_c, humidity_percent, wind_speed_mps, wind_direction_deg,
                   rainfall_mm, chance_of_rain_percent
            FROM {table}
            WHERE {time_column} > NOW() - INTERVAL '1 day'
            AND farm_id = %s
            ORDER BY {time_column} DESC LIMIT 1
            """,
            (farm_id,)
        )
        data = cursor.fetchone() or {}
        print(f"Latest weather data: {data}")

        response = rules_table.query(
            IndexName='StakeholderIndex',
            KeyConditionExpression='farm_id = :fid AND stakeholder = :stake',
            ExpressionAttributeValues={':fid': farm_id, ':stake': stakeholder}
        )
        rules = sorted(response['Items'], key=lambda x: int(x['priority']))
        print("All rules in WeatherRules table:")
        for rule in rules:
            print(f"Rule ID: {rule['rule_id']}, Name: {rule['name']}, Priority: {rule['priority']}, Conditions: {json.dumps(rule['conditions'], cls=DecimalEncoder)}")

        triggered_actions = []
        for rule in rules:
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