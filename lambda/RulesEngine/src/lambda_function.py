# import json
# import boto3
# import psycopg2
# from psycopg2.extras import RealDictCursor
# import os
# from datetime import datetime, timedelta
# import dateutil.parser
# from decimal import Decimal
# import logging
# import math
# from statistics import median
# import requests
# import time
# from urllib.parse import urlparse
# from datetime import datetime, timezone

# # --- Logging Setup ---
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
# current_utc_time = datetime.utcnow().replace(tzinfo=dateutil.tz.UTC)
# logger.info(f"Lambda function initialized at {current_utc_time.isoformat()}Z")

# # --- Custom JSON Encoder ---
# class DecimalEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, Decimal):
#             return float(obj) if obj % 1 else int(obj)
#         if isinstance(obj, datetime):
#             return obj.isoformat()
#         return super().default(obj)

# # --- AWS Clients ---
# dynamodb = boto3.resource('dynamodb')
# sns = boto3.client('sns')
# s3_client = boto3.client('s3')

# # --- DynamoDB Tables ---
# rules_table = dynamodb.Table('Rules')
# reports_table = dynamodb.Table('ScienceTeamReports')

# # --- Environment Configuration ---
# DB_HOST = os.getenv('DB_HOST')
# DB_PORT = os.getenv('DB_PORT', '5432')
# DB_NAME = os.getenv('DB_NAME')
# DB_USER = os.getenv('DB_USER')
# DB_PASSWORD = os.getenv('DB_PASSWORD')
# SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN', 'arn:aws:sns:ap-south-1:580075786360:weather-alerts')
# S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'weather-blogs-2025-science')

# # --- Validation Configurations ---
# VALID_FARM_IDS = {'udaipur_farm1', 'location2', 'location3'}
# VALID_SOURCES = {'openweather', 'weatherapi', 'yrno', 'openmeteo'}
# LOCATIONS = {
#     'udaipur_farm1': {'lat': 24.5854, 'lon': 73.7125},
#     'location2': {'lat': 25.1234, 'lon': 74.5678},
#     'location3': {'lat': 26.4321, 'lon': 75.8765}
# }
# METRIC_MAPPINGS = {
#     'temperature': 'temperature_c',
#     'humidity': 'humidity_percent',
#     'wind_speed': 'wind_speed_mps',
#     'wind_direction': 'wind_direction_deg',
#     'rainfall': 'rainfall_mm',
#     'chance_of_rain': 'chance_of_rain_percent',
#     'heat_index': 'heat_index'
# }
# VALID_OPERATORS = {'>', '<', '=', '>=', '<=', 'delta_gt', 'delta_lt', 'RATE>', 'DAY_DIFF>', 'TIME_WINDOW', 'AND', 'OR', 'NOT', 'SEQUENCE'}
# VALID_NOTIFICATION_TYPES = {'email', 'sms', 'api_callback', 'app'}
# VALID_STAKEHOLDERS = {'farmer', 'operations', 'science_team', 'management', 'field_user'}
# VALID_CONFLICT_RESOLUTIONS = {'first_match', 'highest_priority', 'most_severe'}
# VALIDATION_RANGES = {
#     'temperature_c': (-50, 60),
#     'humidity_percent': (0, 100),
#     'wind_speed_mps': (0, 100),
#     'wind_direction_deg': (0, 360),
#     'rainfall_mm': (0, float('inf')),
#     'chance_of_rain_percent': (0, 100),
#     'heat_index': (-50, 80)
# }

# # --- Severity Thresholds ---
# SEVERITY_THRESHOLDS = {
#     'weather': {
#         'temperature_c': {'low': 32, 'medium': 38, 'high': 42},
#         'humidity_percent': {'low': 70, 'medium': 85, 'high': 95},
#         'rainfall_mm': {'low': 5, 'medium': 25, 'high': 60},
#         'chance_of_rain_percent': {'low': 50, 'medium': 80, 'high': 95},
#         'wind_speed_mps': {'low': 10, 'medium': 18, 'high': 25},
#         'heat_index': {'low': 35, 'medium': 40, 'high': 45}
#     }
# }

# # --- Metric Units ---
# METRIC_UNITS = {
#     'temperature_c': '°C',
#     'humidity_percent': '%',
#     'wind_speed_mps': 'm/s',
#     'wind_direction_deg': '°',
#     'rainfall_mm': 'mm',
#     'chance_of_rain_percent': '%',
#     'heat_index': '°C'
# }

# # --- Retry Settings ---
# MAX_RETRIES = 3
# RETRY_DELAY = 5  # seconds

# def parse_dynamodb_item(item):
#     """Parse a DynamoDB item into standard Python types."""

#     def parse_value(val):
#         if not isinstance(val, dict) or len(val) != 1:
#             return val  # Already parsed or unexpected format

#         dtype, dval = next(iter(val.items()))

#         if dtype == 'S':
#             return dval
#         elif dtype == 'N':
#             try:
#                 return int(dval) if dval.isdigit() else float(dval)
#             except ValueError:
#                 return dval
#         elif dtype == 'BOOL':
#             return dval
#         elif dtype == 'NULL':
#             return None
#         elif dtype == 'L':
#             return [parse_value(v) for v in dval]
#         elif dtype == 'M':
#             return {k: parse_value(v) for k, v in dval.items()}
#         else:
#             return val  # Unknown type fallback

#     return {k: parse_value(v) for k, v in item.items()}


# # --- Geospatial Validation ---
# def validate_location(cursor, farm_id, lon, lat):
#     """Validate farm location against predefined coordinates."""
#     expected = LOCATIONS.get(farm_id)
#     if not expected:
#         return False
#     cursor.execute(
#         "SELECT ST_Distance(ST_SetSRID(ST_MakePoint(%s, %s), 4326), ST_SetSRID(ST_MakePoint(%s, %s), 4326)) AS dist",
#         (lon, lat, expected['lon'], expected['lat'])
#     )
#     dist = cursor.fetchone()['dist']
#     return dist < 1000  # Allow 1km tolerance

# # --- Data Aggregation ---
# def compute_angular_mean(degrees):
#     """Compute mean of angular values (e.g., wind_direction_deg)."""
#     if not degrees:
#         return None
#     radians = [math.radians(d) for d in degrees if d is not None]
#     sin_sum = sum(math.sin(r) for r in radians)
#     cos_sum = sum(math.cos(r) for r in radians)
#     mean_rad = math.atan2(sin_sum, cos_sum)
#     mean_deg = math.degrees(mean_rad) % 360
#     return round(mean_deg, 1)

# def aggregate_weather_data(rows, data_type):
#     """Aggregate weather data from multiple sources using majority voting or median."""
#     if not rows:
#         return None, ["No data available"]

#     metrics = [
#         'temperature_c', 'humidity_percent', 'wind_speed_mps', 'wind_direction_deg', 'rainfall_mm'
#     ] + (['chance_of_rain_percent'] if data_type == 'forecast' else [])
#     aggregated = {}
#     errors = []
#     source_count = len([r for r in rows if r['source'] in VALID_SOURCES])
#     missing_sources = [s for s in VALID_SOURCES if s not in {r['source'] for r in rows}]

#     if source_count < 2:
#         errors.append(f"Insufficient sources: {source_count}/4 (Missing: {', '.join(missing_sources)})")
#         if source_count == 1:
#             # Fallback to single source
#             row = rows[0]
#             for metric in metrics:
#                 aggregated[metric] = float(row[metric]) if row[metric] is not None else None
#             logger.warning(f"Fallback to single source: {row['source']}")
#         else:
#             return None, errors
#     else:
#         for metric in metrics:
#             values = [float(row[metric]) if row[metric] is not None else None for row in rows]
#             valid_values = [v for v in values if v is not None]
#             if len(valid_values) < 2:
#                 aggregated[metric] = None
#                 errors.append(f"Insufficient valid {metric} values: {len(valid_values)}")
#                 continue
#             if metric == 'wind_direction_deg':
#                 aggregated[metric] = compute_angular_mean(valid_values)
#             else:
#                 aggregated[metric] = round(median(valid_values), 2)
#             logger.debug(f"Aggregated {metric}: {aggregated[metric]} from {valid_values}")

#     # Set timestamp or forecast_for
#     if data_type == 'current':
#         aggregated['timestamp'] = rows[0]['timestamp']
#     else:
#         aggregated['forecast_for'] = rows[0]['forecast_for']
#     aggregated['source_count'] = source_count

#     return aggregated, errors

# # --- Virtual Sensors ---
# def compute_virtual_sensors(data):
#     """Compute virtual sensor values (e.g., heat_index)."""
#     if 'temperature_c' in data and 'humidity_percent' in data and data['temperature_c'] is not None and data['humidity_percent'] is not None:
#         temp_c = float(data['temperature_c'])
#         humidity = float(data['humidity_percent'])
#         heat_index = temp_c + 0.33 * humidity - 40
#         data['heat_index'] = round(heat_index, 2)
#     return data

# # --- Data Validation ---
# def validate_data(data, rule_type, data_type):
#     """Validate aggregated weather data against allowed ranges."""
#     if rule_type != 'data':
#         return data, [f"Invalid rule_type: {rule_type}"]
    
#     errors = []
#     valid_metrics = {
#         'current': {'temperature_c', 'humidity_percent', 'wind_speed_mps', 'wind_direction_deg', 'rainfall_mm', 'heat_index'},
#         'forecast': {'temperature_c', 'humidity_percent', 'wind_speed_mps', 'wind_direction_deg', 'rainfall_mm', 'chance_of_rain_percent', 'heat_index'}
#     }.get(data_type, set())
    
#     for metric in valid_metrics:
#         if metric in data and data[metric] is not None:
#             min_val, max_val = VALIDATION_RANGES.get(metric, (-float('inf'), float('inf')))
#             try:
#                 value = float(data[metric])
#                 if not (min_val <= value <= max_val):
#                     logger.warning(f"Invalid {metric} value {value} for {rule_type}/{data_type}")
#                     errors.append(f"{metric}: {value} outside range [{min_val}, {max_val}]")
#                     data[metric] = None
#             except (ValueError, TypeError):
#                 errors.append(f"{metric}: non-numeric value '{data[metric]}'")
#                 data[metric] = None
    
#     return compute_virtual_sensors(data), errors


# # --- Rule Validation ---
# def validate_rule(rule):
#     """Validate a rule from DynamoDB."""
#     try:
#         required_fields = {'rule_id', 'rule_type', 'conditions', 'actions', 'farm_scope'}
#         missing_fields = required_fields - set(rule.keys())
#         if missing_fields:
#             logger.error(f"Rule {rule.get('rule_id', 'unknown')} invalid: missing {missing_fields}")
#             return False, f"Missing fields: {', '.join(missing_fields)}"

#         if rule['rule_type'] != 'weather':
#             logger.error(f"Rule {rule['rule_id']} invalid: rule_type must be 'weather'")
#             return False, "rule_type must be 'weather'"
#         if rule['farm_scope']['scope_type'] not in {'single', 'all'}:
#             logger.error(f"Rule {rule['rule_id']} invalid: farm_scope_type must be 'single' or 'all'")
#             return False, "farm_scope_type must be 'single' or 'all'"
#         if rule['farm_scope']['scope_type'] == 'single' and not rule['farm_scope']['farm_ids']:
#             logger.error(f"Rule {rule['rule_id']} invalid: farm_ids required for single scope")
#             return False, "farm_ids required for single scope"
#         if 'target_role' in rule and rule['target_role'] not in VALID_STAKEHOLDERS:
#             logger.error(f"Rule {rule['rule_id']} invalid: target_role {rule['target_role']}")
#             return False, f"Invalid target_role: {rule['target_role']}"

#         def check_conditions(conditions):
#             if isinstance(conditions, list):
#                 return all(check_conditions(cond)[0] for cond in conditions), []

#             operator = conditions.get('operator')
#             if operator not in VALID_OPERATORS:
#                 return False, f"Invalid operator: {operator}"

#             if operator in {'AND', 'OR', 'NOT', 'SEQUENCE'}:
#                 sub_conditions = conditions.get('sub_conditions', [])
#                 if not sub_conditions:
#                     return False, f"No sub_conditions for {operator}"
#                 if operator == 'NOT' and len(sub_conditions) != 1:
#                     return False, "NOT requires one sub_condition"
#                 if operator == 'SEQUENCE':
#                     for i, cond in enumerate(sub_conditions):
#                         if i % 2 == 1 and 'within' not in cond:
#                             return False, "'within' missing in SEQUENCE"
#                         elif i % 2 == 0 and 'within' in cond:
#                             return False, "'within' in condition"
#                 results = [check_conditions(cond) for cond in sub_conditions]
#                 valid = all(r[0] for r in results)
#                 errors = [err for r in results for err in r[1]]
#                 return valid, errors

#             if 'parameter' not in conditions or conditions['parameter'] not in METRIC_MAPPINGS:
#                 return False, f"Invalid parameter: {conditions.get('parameter')}"
#             if operator in {'delta_gt', 'delta_lt'} and 'reference' not in conditions:
#                 return False, f"Reference missing for {operator}"
#             if operator in {'>', '<', '=', '>=', '<=', 'delta_gt', 'delta_lt', 'RATE>', 'DAY_DIFF>'} and 'value' not in conditions:
#                 return False, f"Value missing for {operator}"
#             return True, []

#         valid, condition_errors = check_conditions(rule['conditions'])
#         if not valid:
#             logger.error(f"Rule {rule['rule_id']} invalid: {condition_errors}")
#             return False, f"Condition errors: {', '.join(condition_errors)}"

#         for action in rule.get('actions', []):
#             if 'action_type' not in action or action['action_type'] != 'notification':
#                 logger.error(f"Rule {rule['rule_id']} invalid: action_type must be 'notification'")
#                 return False, "action_type must be 'notification'"
#             if 'channel' not in action or action['channel'] not in VALID_NOTIFICATION_TYPES:
#                 logger.error(f"Rule {rule['rule_id']} invalid: channel {action.get('channel')}")
#                 return False, f"Invalid channel: {action.get('channel')}"
#             if 'message' not in action:
#                 logger.error(f"Rule {rule['rule_id']} invalid: message missing")
#                 return False, "message missing in action"
#             if 'priority' not in action or action['priority'] not in {'low', 'medium', 'high'}:
#                 logger.error(f"Rule {rule['rule_id']} invalid: priority {action.get('priority')}")
#                 return False, f"Invalid priority: {action.get('priority')}"

#         logger.info(f"Rule {rule['rule_id']} is valid")
#         return True, ""
#     except Exception as e:
#         logger.error(f"Error validating rule {rule.get('rule_id', 'unknown')}: {str(e)}")
#         return False, f"Validation error: {str(e)}"

# # --- Data Fetching ---
# def get_historical_trend(cursor, metric, farm_id, table, time_offset):
#     """Fetch historical data for trend analysis."""
#     try:
#         time_column = 'forecast_for' if 'forecast' in table else 'timestamp'
#         cursor.execute(
#             f"""
#             SELECT AVG({metric}) as avg_value
#             FROM {table}
#             WHERE farm_id = %s AND {time_column} > NOW() AT TIME ZONE 'UTC' - INTERVAL %s
#             """,
#             (farm_id, time_offset)
#         )
#         result = cursor.fetchone()
#         logger.debug(f"Historical trend for {metric} at {farm_id}: {result}")
#         return float(result['avg_value']) if result and result['avg_value'] is not None else None
#     except Exception as e:
#         logger.error(f"Error fetching trend for {farm_id}/{metric}: {e}")
#         return None

# def get_table_name(source):
#     """Get PostgreSQL table name based on rule source."""
#     return 'forecast_weather' if source == 'forecast' else 'current_weather'

# # --- Condition Evaluation ---
# def evaluate_condition(data, condition, table, farm_id, cursor, rule_id):
#     """Evaluate a single condition."""
#     try:
#         metric = METRIC_MAPPINGS.get(condition['parameter'])
#         operator = condition['operator']
#         value = float(condition['value']) if 'value' in condition else None
#         time_column = 'forecast_for' if 'forecast' in table else 'timestamp'
#         if operator == 'TIME_WINDOW':
#             threshold = condition['threshold']
#             min_duration = condition['temporal']['min_duration']
#             window = condition['temporal']['window']
#             cursor.execute(
#                 f"""
#                 SELECT COUNT(*) FROM {table}
#                 WHERE {metric} {threshold['operator']}%s
#                 AND {time_column} > NOW() AT TIME ZONE 'UTC' - INTERVAL %s
#                 AND farm_id = %s
#                 """,
#                 (float(threshold['value']), window, farm_id)
#             )
#             result = cursor.fetchone()
#             if not result:
#                 logger.info(f"TIME_WINDOW: No data for {metric} in {table}")
#                 return False, "No data available"
#             count = result['count']
#             min_count = int(min_duration.split()[0]) if 'hour' in min_duration else int(min_duration.split()[0]) // 60
#             logger.debug(f"TIME_WINDOW: {metric} {threshold['operator']} for {min_duration} in {window}, count: {count}, required: {min_count}")
#             return count >= min_count, f"Count: {count}, required: {min_count}"

#         if operator == 'RATE>':
#             interval = condition['temporal']['interval']
#             cursor.execute(
#                 f"""
#                 SELECT {metric}, {time_column} FROM {table}
#                 WHERE farm_id = %s AND {time_column} <= NOW() AT TIME ZONE 'UTC'
#                 ORDER BY {time_column} DESC LIMIT 2
#                 """,
#                 (farm_id,)
#             )
#             rows = cursor.fetchall()
#             if len(rows) < 2:
#                 logger.info(f"Rate-of-change for {metric}: Not enough data")
#                 return False, "Not enough data for rate calculation"
#             time_diff = float((rows[0][time_column] - rows[1][time_column]).total_seconds()/3600)
#             value_diff = float(rows[0][metric]) - float(rows[1][metric])
#             rate = value_diff / time_diff if time_diff != 0 else 0
#             expected_rate = float(value) / (float(interval.split()[0]) / 60 if 'minute' in interval else float(interval.split()[0]))
#             logger.debug(f"Rate for {metric}: {rate} vs {expected_rate}")
#             return rate > expected_rate, f"Rate: {rate:.2f}, expected: {expected_rate:.2f}"

#         if operator == 'DAY_DIFF>':
#             day1 = condition['temporal']['day1']
#             day2 = condition['temporal']['day2']
#             now = datetime.utcnow().replace(tzinfo=dateutil.tz.UTC)
#             day1_date = now if day1 == 'today' else now + timedelta(days=1) if day1 == 'tomorrow' else now + timedelta(days=int(day1.split('_')[1]))
#             day2_date = now if day2 == 'today' else now + timedelta(days=1) if day2 == 'tomorrow' else now + timedelta(days=int(day2.split('_')[1]))
#             day1_start = day1_date.replace(hour=0, minute=0, second=0, microsecond=0)
#             day1_end = day1_date.replace(hour=23, minute=59, second=59, microsecond=999999)
#             day2_start = day2_date.replace(hour=0, minute=0, second=0, microsecond=0)
#             day2_end = day2_date.replace(hour=23, minute=59, second=59, microsecond=999999)
#             cursor.execute(
#                 f"""
#                 SELECT AVG({metric}) as avg_value
#                 FROM {table}
#                 WHERE farm_id = %s AND {time_column} BETWEEN %s AND %s
#                 """,
#                 (farm_id, day1_start, day1_end)
#             )
#             result = cursor.fetchone()
#             if not result or result['avg_value'] is None:
#                 logger.warning(f"No data for {metric} on {day1}")
#                 return False, f"No data for {day1_date}"
#             day1_avg = float(result['avg_value'])
#             cursor.execute(
#                 f"""
#                 SELECT AVG({metric}) as avg_value
#                 FROM {table}
#                 WHERE farm_id = %s AND {time_column} BETWEEN %s AND %s
#                 """,
#                 (farm_id, day2_start, day2_end)
#             )
#             result = cursor.fetchone()
#             if not result or result['avg_value'] is None:
#                 logger.warning(f"No data for {day2}")
#                 return False, f"No data for {day2_date}"
#             day2_avg = float(result['avg_value'])
#             diff = day2_avg - day1_avg
#             logger.debug(f"Day diff for {metric}: {day2_avg} - {day1_avg} = {diff} vs {value}")
#             return diff > value, f"Difference: {diff:.2f}, required: {value}"

#         if operator == 'delta_gt':
#             time_offset = condition['reference']['time_offset']
#             ref_metric = METRIC_MAPPINGS.get(condition['reference']['parameter'])
#             cursor.execute(
#                 f"""
#                 SELECT {ref_metric} FROM {table}
#                 WHERE farm_id = %s AND {time_column} <= NOW() AT TIME ZONE 'UTC' - INTERVAL %s
#                 ORDER BY {time_column} DESC LIMIT 1
#                 """,
#                 (farm_id, time_offset)
#             )
#             ref_result = cursor.fetchone()
#             if not ref_result:
#                 logger.info(f"No reference data for {ref_metric} at offset {time_offset}")
#                 return False, f"No reference for {time_offset}"
#             ref_value = float(ref_result[ref_metric])
#             latest_value = float(data.get(metric)) if data.get(metric) is not None else None
#             if latest_value is None:
#                 logger.info(f"No data for {metric} in rule {rule_id}")
#                 return False, "No valid data for {metric}"
#             delta = latest_value - ref_value
#             logger.debug(f"Delta: {latest_value} - {ref_value} = {delta} vs {value}")
#             return delta > value, f"Delta: {delta:.2f}, required: {value}"

#         if operator == 'delta_lt':
#             time_offset = condition['reference']['time_offset']
#             ref_metric = METRIC_MAPPINGS.get(condition['reference']['parameter'])
#             cursor.execute(
#                 f"""
#                 SELECT {ref_metric} FROM {table}
#                 WHERE farm_id = %s AND {time_column} <= NOW() AT TIME ZONE 'UTC' - INTERVAL %s
#                 ORDER BY {time_column} DESC LIMIT 1
#                 """,
#                 (farm_id, time_offset)
#             )
#             ref_result = cursor.fetchone()
#             if not ref_result:
#                 logger.info(f"No reference data for {ref_metric} at offset {time_offset}")
#                 return False, f"No reference data for{time_offset}"
#             ref_value = float(ref_result[ref_metric])
#             latest_value = float(data.get(metric)) if data.get(metric) is not None else None
#             if latest_value is None:
#                 logger.info(f"No data for {metric} in rule {rule_id}")
#                 return False, f"No valid datafor {metric}"
#             delta = latest_value - ref_value
#             logger.debug(f"Delta: {latest_value} - {ref_value} = {delta} vs {value}")
#             return delta < value, f"Delta: {delta:.2f}, required: < {value}"
#         latest_value = float(data.get(metric)) if data.get(metric) is not None else None
#         if latest_value is None:
#             logger.warning(f"No data for {metric} in rule {rule_id}")
#             return False, f"No value for {metric}"
#         if operator == '>':
#             return latest_value > value, f"{metric}: {latest_value:.2f} > {value:.2f}"
#         elif operator == '<':
#             return latest_value < value, f"{metric}: {latest_value:.2f} < {value:.2f}"
#         elif operator == '==':
#             return latest_value == value, f"{metric}: {latest_value:.2f} == {value:.2f}"
#         elif operator == '>=':
#             return latest_value >= value, f"{metric}: {latest_value:.2f} >= {value:.2f}"
#         elif operator == '<=':
#             return latest_value <= value, f"{metric}: {latest_value:.2f} <= {value:.2f}"
#         return False, f"Invalid operator: {operator}"
#     except Exception as e:
#         logger.error(f"Error evaluating condition in rule {rule_id}: {str(e)}")
#         return False, f"Evaluation error: {str(e)}"
# def evaluate_sequence(data, condition, table, farm_id, cursor, rule_id):
#     """Evaluate a SEQUENCE condition with optional time gaps between events."""
#     try:
#         sub_conditions = condition['sub_conditions']
#         time_column = 'forecast_for' if 'forecast' in table else 'timestamp'
#         last_time = None
#         max_interval = None

#         for i, cond in enumerate(sub_conditions):
#             # 'within' clause
#             if i % 2 == 1:
#                 max_interval = cond['within']
#                 continue

#             metric = METRIC_MAPPINGS.get(cond['parameter'])
#             if not metric:
#                 logger.warning(f"Unknown metric: {cond['parameter']} in rule {rule_id}")
#                 return False, f"Unknown metric: {cond['parameter']}"

#             # Fetch the first time this condition was met
#             cursor.execute(
#                 f"""
#                 SELECT {time_column} FROM {table}
#                 WHERE {metric} {cond['operator']} %s
#                 AND farm_id = %s AND {time_column} > NOW() AT TIME ZONE 'UTC' - INTERVAL '24 hours'
#                 ORDER BY {time_column} ASC LIMIT 1
#                 """,
#                 (float(cond['value']), farm_id)
#             )
#             result = cursor.fetchone()
#             if not result:
#                 logger.info(f"Sequence failed in rule {rule_id}: {metric} {cond['operator']} {cond.get('value', 'N/A')}")
#                 return False, f"No data for {metric}: {cond['operator']} {cond.get('value', 'N/A')}"

#             current_time = result[time_column]

#             # If a previous condition was matched, check time difference
#             if last_time and max_interval:
#                 # Convert interval like "30 minutes" to minutes
#                 interval_parts = max_interval.split()
#                 interval_minutes = int(interval_parts[0]) if 'minute' in interval_parts[1] else int(interval_parts[0]) * 60
#                 time_diff = (current_time - last_time).total_seconds() / 60
#                 if time_diff > interval_minutes:
#                     logger.info(f"Sequence failed in rule {rule_id}: Time gap {time_diff:.2f} > allowed {interval_minutes} minutes")
#                     return False, f"Time gap: {time_diff:.2f} minutes exceeds {max_interval}"
#             last_time = current_time

#         logger.debug(f"Sequence passed for rule {rule_id}")
#         return True, "Sequence conditions met"
#     except Exception as e:
#         logger.error(f"Error evaluating sequence in rule {rule_id}: {str(e)}")
#         return False, f"Sequence error: {str(e)}"

# def evaluate_conditions(data, conditions, table, farm_id, cursor, rule_id):
#     """Evaluate conditions recursively."""
#     try:
#         # Handle list of conditions (implies AND)
#         if isinstance(conditions, list):
#             results = [evaluate_conditions(data, cond, table, farm_id, cursor, rule_id) for cond in conditions]
#             return all(r[0] for r in results), [r[1] for r in results if r[1]]

#         operator = conditions.get('operator')
#         sub_conditions = conditions.get('sub_conditions', [])

#         # AND logic
#         if operator == 'AND':
#             results = []
#             for cond in sub_conditions:
#                 if 'parameter' in cond:
#                     result = evaluate_condition(data, cond, table, farm_id, cursor, rule_id)
#                 else:
#                     result = evaluate_conditions(data, cond, table, farm_id, cursor, rule_id)
#                 results.append(result)
#             return all(r[0] for r in results), [r[1] for r in results if r[1]]

#         # OR logic
#         elif operator == 'OR':
#             results = []
#             for cond in sub_conditions:
#                 if 'parameter' in cond:
#                     result = evaluate_condition(data, cond, table, farm_id, cursor, rule_id)
#                 else:
#                     result = evaluate_conditions(data, cond, table, farm_id, cursor, rule_id)
#                 results.append(result)
#             return any(r[0] for r in results), [r[1] for r in results if r[1]]

#         # NOT logic
#         elif operator == 'NOT':
#             if not sub_conditions or not isinstance(sub_conditions, list):
#                 return False, ["NOT operator needs one sub_condition"]
#             result = evaluate_conditions(data, sub_conditions[0], table, farm_id, cursor, rule_id)
#             return not result[0], [f"NOT: {result[1]}" if result[1] else []]

#         # Sequence or other special operator handling (e.g. != might map to sequence)
#         elif operator == '!=' or operator == 'SEQUENCE':
#             return evaluate_sequence(data, conditions, table, farm_id, cursor, rule_id), []

#         # Default: evaluate single condition
#         return evaluate_condition(data, conditions, table, farm_id, cursor, rule_id)

#     except Exception as e:
#         logger.error(f"Error evaluating conditions in rule {rule_id}: {str(e)}")
#         return False, [f"Conditions error: {str(e)}"]
# # --- Notification Logic ---

# def determine_severity(rule_type, metric, value, delta=None):
#     """Determine severity based on metric and thresholds."""
#     thresholds = SEVERITY_THRESHOLD.get(rule_type, {}).get(metric, {})
#     if not thresholds:
#         return 'low'

#     # Convert value to float if needed
#     if isinstance(value, (Decimal, str)):
#         value = float(value)

#     # Threshold comparison
#     if value >= thresholds.get('high', float('inf')):
#         severity = 'high'
#     elif value >= thresholds.get('medium', float('inf')):
#         severity = 'medium'
#     elif value >= thresholds.get('low', float('inf')):
#         severity = 'low'
#     else:
#         severity = 'low'

#     # Delta-based severity override
#     if delta is not None:
#         delta_thresholds = {
#             'temperature_c': 5,
#             'humidity_percent': 20,
#             'rainfall_mm': 30,
#             'heat_index': 5
#         }
#         if metric in delta_thresholds and abs(delta) > delta_thresholds[metric]:
#             if severity == 'low':
#                 severity = 'medium'
#             elif severity == 'medium':
#                 severity = 'high'

#     return severity


# def compose_message(action, rule, data_dict, farm_id, data_type, severity, metric, value, timestamp, delta=None, time_offset=None, source_count=0):
#     """Generate dynamic notification message."""
#     message_template = action.get('message', '')
#     unit = METRIC_UNITS.get(metric, {}).get('unit', '')

#     # Extract parameter
#     if isinstance(rule.get('conditions'), list):
#         parameter = rule['conditions'][0].get('parameter', 'unknown')
#     else:
#         parameter = rule.get('conditions', {}).get('parameter', 'unknown')

#     # Fill replacements
#     replacements = {
#         '{farm_id}': str(farm_id),
#         '{rule_name}': rule.get('rule_name', 'Unknown Rule'),
#         '{severity}': severity.capitalize(),
#         '{timestamp}': timestamp.isoformat(),
#         '{parameter}': parameter,
#         '{value}': str(value),
#         '{unit}': unit,
#         '{delta}': str(delta) if delta is not None else '',
#         '{time_offset}': str(time_offset) if time_offset else '',
#         '{source_count}': str(source_count)
#     }

#     message = message_template
#     for placeholder, replacement in replacements.items():
#         message = message.replace(placeholder, replacement)

#     return message

# def select_channels(stakeholder, severity, priority, timestamp):
#     """Select notification channels based on stakeholder and time."""
#     """
#     current_hour = timestamp.hour
#     is_night = 0 <= current_hour < 6
#     stakeholder = stakeholder or 'default'

#     if stakeholder == 'farmer':
#         return ['sms', 'app'] if priority == 'high' and not is_night else ['app']
#     elif stakeholder == 'operations':
#         return ['sms', 'email'] if priority == 'high' and not is_night else ['email']
#     elif stakeholder == 'science_team':
#         return ['email']
#     elif stakeholder == 'management':
#         return ['email', 'api_callback'] if priority in ['medium', 'high'] else ['email']
#     elif stakeholder == 'field_user' or stakeholder == 'default':
#         return ['sms', 'app'] if priority == 'high' and not is_night else ['email', 'app']
#     return ['email']


# def send_notification(channel, message, priority, severity, rule_name, timestamp, endpoint=None, retries=0):

#     try:
#         if isinstance(timestamp, str):
#             timestamp = datetime.fromisoformat(timestamp)

#         if channel == 'sms':
#             response = sns.publish(
#                 TopicArn=SNS_TOPIC_ARN,
#                 Message=message,
#                 MessageAttributes={'channel': {'DataType': 'String', 'StringValue': 'sms'}}
#             )
#             logger.info(f"Sent SMS: {message}, SNS Message ID: {response['MessageId']}")

#         elif channel == 'email':
#             subject = f"Weather {rule_name} ({severity.capitalize()}) at {timestamp.isoformat()}"
#             response = sns.publish(
#                 TopicArn=SNS_TOPIC_ARN,
#                 Message=message,
#                 Subject=subject,
#                 MessageAttributes={'channel': {'DataType': 'String', 'StringValue': 'email'}}
#             )
#             logger.info(f"Sent email: {message}, SNS Message ID: {response['MessageId']}")

#         elif channel == 'api_callback' and endpoint:
#             parsed = urlparse(endpoint)
#             if parsed.scheme not in ['http', 'https'] or not parsed.netloc:
#                 raise ValueError(f"Invalid callback URL: {endpoint}")
#             response = requests.post(endpoint, json={'message': message, 'severity': severity}, timeout=10)
#             response.raise_for_status()
#             logger.info(f"API callback sent to {endpoint}: {message}")

#         elif channel == 'app':
#             logger.info(f"App notification queued: {message}")

#         return True

#     except Exception as e:
#         if retries < MAX_RETRIES:
#             logger.warning(f"Retrying {channel} notification ({retries + 1}) due to error: {e}")
#             time.sleep(RETRY_DELAY)
#             return send_notification(channel, message, priority, severity, rule_name, timestamp, endpoint, retries + 1)

#         logger.error(f"Failed to send {channel} after {MAX_RETRIES} retries: {str(e)}")
#         return False
# # def generate_daily_summary(cursor, farm_id):
# #     try:
# #         if farm_id not in VALID_FARM_IDS:
# #             logger.error(f"Invalid farm_id: {farm_id}")
# #             return False

# #         date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
# #         timestamp = datetime.now(timezone.utc).isoformat()

# #         # --- SQL Query to Get Daily Weather Summary ---
# #         interval_hours = 24
# # cursor.execute(
# #     """
# #     SELECT 
# #         AVG(temperature_c) AS avg_temp,
# #         MAX(temperature_c) AS max_temp,
# #         MIN(temperature_c) AS min_temp,
# #         AVG(humidity_percent) AS avg_humidity,
# #         MAX(humidity_percent) AS max_humidity,
# #         MIN(humidity_percent) AS min_humidity,
# #         SUM(rainfall_mm) AS total_rain,
# #         AVG(wind_speed_mps) AS avg_wind_speed
# #     FROM current_weather_data
# #     WHERE farm_id = %s AND timestamp >= NOW() - INTERVAL %s
# #     """,
# #     (farm_id, f"{interval_hours} hours")
# # )

# #             WHERE farm_id = %s AND timestamp >= NOW() - INTERVAL '{interval_hours} hours'
# #             """,
# #             (farm_id,)
# #         )
# #         stats = cursor.fetchone() or {}

# #         def fmt(x): return f"{x:.2f}" if x is not None else 'N/A'

# #         avg_temp = fmt(stats.get('avg_temp'))
# #         max_temp = fmt(stats.get('max_temp'))
# #         min_temp = fmt(stats.get('min_temp'))
# #         avg_humidity = fmt(stats.get('avg_humidity'))
# #         max_humidity = fmt(stats.get('max_humidity'))
# #         min_humidity = fmt(stats.get('min_humidity'))
# #         total_rain = fmt(stats.get('total_rain'))
# #         avg_wind = fmt(stats.get('avg_wind_speed'))

# #         # --- Construct JSON summary ---
# #         summary_data = {
# #             "farm_id": farm_id,
# #             "date": date,
# #             "timestamp": timestamp,
# #             "summary": {
# #                 "avg_temp": avg_temp,
# #                 "max_temp": max_temp,
# #                 "min_temp": min_temp,
# #                 "avg_humidity": avg_humidity,
# #                 "max_humidity": max_humidity,
# #                 "min_humidity": min_humidity,
# #                 "total_rain": total_rain,
# #                 "avg_wind_speed": avg_wind
# #             }
# #         }

# #         # --- Store summary JSON to S3 ---
# #         s3_key = f"summaries/{farm_id}/{date}/weather_summary.json"
# #         s3_client.put_object(
# #             Bucket=S3_BUCKET_NAME,
# #             Key=s3_key,
# #             Body=json.dumps(summary_data, indent=2),
# #             ContentType='application/json'
# #         )

# #         logger.info(f"Weather summary stored in S3: s3://{S3_BUCKET_NAME}/{s3_key}")
# #         return True

# #     except Exception as e:
# #         logger.error(f"Error generating daily summary for {farm_id}: {str(e)}")
# #         return False

# # --- Main Handler ---
# from datetime import datetime, timezone

# # --- Main Handler ---
# def lambda_handler(event, context):
#     """Main Lambda handler for rule evaluation."""
#     conn = None
#     cursor = None
#     timestamp = datetime.now(timezone.utc)

#     try:
#         # Database connection
#         for attempt in range(1, MAX_RETRIES + 1):
#             try:
#                 conn = psycopg2.connect(
#                     host=DB_HOST,
#                     port=DB_PORT,
#                     database=DB_NAME,
#                     user=DB_USER,
#                     password=DB_PASSWORD,
#                     cursor_factory=RealDictCursor,
#                     connect_timeout=10
#                 )
#                 cursor = conn.cursor()
#                 logger.info("Database connection successful")
#                 break
#             except psycopg2.Error as e:
#                 logger.error(f"Database connection attempt {attempt}/{MAX_RETRIES} failed: {e}")
#                 if attempt == MAX_RETRIES:
#                     return {
#                         'statusCode': 500,
#                         'body': json.dumps({'error': 'Database connection failed'}, cls=DecimalEncoder)
#                     }
#                 time.sleep(RETRY_DELAY)

#         # Parse event
#         farm_id = event.get('farm_id', 'udaipur_farm1')
#         if farm_id not in VALID_FARM_IDS:
#             return {
#                 'statusCode': 400,
#                 'body': json.dumps({'error': f"Invalid farm_id: {farm_id}"}, cls=DecimalEncoder)
#             }

#         # --- Test Mode ---
#         if event.get('test_mode'):
#             try:
#                 test_results = []
#                 test_time_range = event.get('test_time_range', '24 hours')
#                 test_sources = event.get('test_sources', list(VALID_TYPES))

#                 response = rules_table.scan()
#                 rules = [parse_dynamodb_item(item) for item in response.get('Items', [])]
#                 valid_rules = [
#                     r for r in rules if r.get('active', True) and (
#                         r['farm_scope']['scope_type'] == 'all' or
#                         farm_id in r.get('farm_scope', {}).get('farm_ids', [])
#                     )
#                 ]

#                 for rule in valid_rules:
#                     is_valid, validation_error = validate_rule(rule)
#                     if not is_valid:
#                         test_results.append({
#                             'rule_id': rule['rule_id'],
#                             'status': 'Invalid rule',
#                             'description': validation_error,
#                             'conditions': rule['conditions']
#                         })
#                         continue

#                     source = (
#                         rule['conditions'][0].get('source', 'current') if isinstance(rule['conditions'], list)
#                         else rule['conditions'].get('source', 'current')
#                     )
#                     table = get_table_name(source)
#                     time_column = 'timestamp'
#                     source_filter = ' AND source IN %s' if test_sources else ''

#                     cursor.execute(
#                         f"""
#                         SELECT 
#                             source, {time_column}, temperature_c, humidity_percent,
#                             wind_speed_mps, wind_direction_deg, rainfall_mm,
#                             chance_of_rain,
#                             ST_X(location) AS longitude,
#                             ST_Y(location) AS latitude
#                         FROM {table}
#                         WHERE farm_id = %s AND {time_column} >= NOW() - INTERVAL %s
#                             {source_filter}
#                         ORDER BY {time_column} DESC, source
#                         """,
#                         (farm_id, test_time_range, tuple(test_sources)) if test_sources else (farm_id, test_time_range)
#                     )
#                     rows = cursor.fetchall()

#                     if not rows:
#                         test_results.append({
#                             'rule_id': rule['rule_id'],
#                             'status': 'Not triggered',
#                             'description': f"No {source} data for {farm_id} in last {test_time_range}",
#                             'conditions': rule['conditions']
#                         })
#                         continue

#                     valid_rows = [r for r in rows if validate_location(cursor, farm_id, r['longitude'], r['latitude'])]
#                     if not valid_rows:
#                         test_results.append({
#                             'rule_id': rule['rule_id'],
#                             'status': 'Not triggered',
#                             'description': f"Invalid location data for {farm_id}",
#                             'conditions': rule['conditions']
#                         })
#                         continue

#                     data, agg_errors = aggregate_weather_data(valid_rows, source)
#                     if not data:
#                         test_results.append({
#                             'rule_id': rule['rule_id'],
#                             'status': 'Not triggered',
#                             'description': f"Data aggregation failed: {'; '.join(agg_errors)}",
#                             'conditions': rule['conditions']
#                         })
#                         continue

#                     data_dict, validation_errors = validate_data(data, rule['rule_type'], source)
#                     errors = agg_errors + validation_errors

#                     triggered, eval_results = evaluate_conditions(data_dict, rule['conditions'], table, farm_id, cursor, rule['rule_id'])
#                     result = {
#                         'rule_id': rule['rule_id'],
#                         'status': 'Triggered' if triggered else 'Not triggered',
#                         'description': f"Rule evaluated: {'; '.join(eval_results) if eval_results else 'Conditions met' if triggered else 'Conditions not met'}",
#                         'conditions': data_dict,
#                         'data': {
#                             'timestamp': data_dict.get('timestamp', data_dict.get('forecast_for')).isoformat(),
#                             'temperature_c': float(data_dict.get('temperature_c')) if data_dict.get('temperature_c') is not None else None,
#                             'humidity_percent': float(data_dict.get('humidity_percent')) if data_dict.get('humidity_percent') is not None else None,
#                             'wind_speed_mps': float(data_dict.get('wind_speed_mps')) if data_dict.get('wind_speed_mps') is not None else None,
#                             'wind_direction_deg': float(data_dict.get('wind_direction_deg')) if data_dict.get('wind_direction_deg') is not None else None,
#                             'rainfall_mm': float(data_dict.get('rainfall_mm')) if data_dict.get('rainfall_mm') is not None else None,
#                             'chance_of_rain_percent': float(data_dict.get('chance_of_rain_percent')) if data_dict.get('chance_of_rain_percent') is not None else None,
#                             'heat_index': float(data_dict.get('heat_index')) if data_dict.get('heat_index') is not None else None,
#                             'source_count': data_dict.get('source_count', 0),
#                             'sources_used': [r['source'] for r in valid_rows]
#                         },
#                         'errors': errors if errors else []
#                     }
#                     test_results.append(result)

#                 return {
#                     'statusCode': 200,
#                     'body': json.dumps({'test_results': test_results}, cls=DecimalEncoder)
#                 }
#             except Exception as e:
#                 logger.error(f"Test mode error: {str(e)}")
#                 return {
#                     'statusCode': 500,
#                     'body': json.dumps({'error': f"Test mode failed: {str(e)}"}, cls=DecimalEncoder)
#                 }

#         # --- Normal Mode ---
#         try:
#             response = rules_table.scan()
#             rules = [parse_dynamodb_item(item) for item in response.get('Items', [])]
#             valid_rules = [
#                 r for r in rules if r.get('active', True) and (
#                     r['farm_scope']['scope_type'] == 'all' or
#                     farm_id in r.get('farm_scope', {}).get('farm_ids', [])
#                 )
#             ]

#             triggered_actions = []
#             for rule in valid_rules:
#                 is_valid, validation_error = validate_rule(rule)
#                 if not is_valid:
#                     logger.warning(f"Skipping invalid rule {rule['rule_id']}: {validation_error}")
#                     continue

#                 source = (
#                     rule['conditions'][0].get('source', 'current') if isinstance(rule['conditions'], list)
#                     else rule['conditions'].get('source', 'current')
#                 )
#                 table = get_table_name(source)
#                 time_column = 'timestamp'

#                 cursor.execute(
#                     f"""
#                     SELECT 
#                         source, {time_column}, temperature_c, humidity_percent,
#                         wind_speed_mps, wind_direction_deg, rainfall_mm,
#                         chance_of_rain,
#                         ST_X(location) AS longitude,
#                         ST_Y(location) AS latitude
#                     FROM {table}
#                     WHERE farm_id = %s AND {time_column} <= NOW() AT TIME ZONE 'UTC'
#                     ORDER BY {time_column} DESC, source
#                     """,
#                     (farm_id,)
#                 )
#                 rows = cursor.fetchall()
#                 if not rows:
#                     logger.warning(f"No {source} data for {farm_id} in rule {rule['rule_id']}")
#                     continue

#                 valid_rows = [r for r in rows if validate_location(cursor, farm_id, r['longitude'], r['latitude'])]
#                 if not valid_rows:
#                     logger.warning(f"Invalid location data for {farm_id} in rule {rule['rule_id']}")
#                     continue

#                 data, agg_errors = aggregate_weather_data(valid_rows, source)
#                 if not data:
#                     logger.warning(f"Data aggregation failed for rule {rule['rule_id']}: {agg_errors}")
#                     continue

#                 data_dict, validation_errors = validate_data(data, rule['rule_type'], source)

#                 triggered, eval_results = evaluate_conditions(data_dict, rule['conditions'], table, farm_id, cursor, rule['rule_id'])
#                 if triggered:
#                     metric = METRIC_MAPPINGS[
#                         rule['conditions'][0]['parameter'] if isinstance(rule['conditions'], list)
#                         else rule['conditions']['parameter']
#                     ]
#                     value = data_dict.get(metric)
#                     delta = None
#                     time_offset = None
#                     if (
#                         (isinstance(rule['conditions'], list) and rule['conditions'][0]['operator'] in {'delta_gt', 'delta_lt'}) or
#                         (not isinstance(rule['conditions'], list) and rule['conditions']['operator'] in {'delta_gt', 'delta_lt'})
#                     ):
#                         time_offset = (
#                             rule['conditions'][0]['reference']['time_offset'] if isinstance(rule['conditions'], list)
#                             else rule['conditions']['reference']['time_offset']
#                         )
#                         ref_metric = METRIC_MAPPINGS[
#                             rule['conditions'][0]['reference']['parameter'] if isinstance(rule['conditions'], list)
#                             else rule['conditions']['reference']['parameter']
#                         ]
#                         cursor.execute(
#                             f"""
#                             SELECT {ref_metric} 
#                             FROM {table} 
#                             WHERE farm_id = %s AND {time_column} <= NOW() - INTERVAL %s
#                             ORDER BY {time_column} DESC 
#                             LIMIT %s
#                             """,
#                             (farm_id, time_offset, 1)
#                         )
#                         ref_result = cursor.fetchone()
#                         if ref_result:
#                             delta = float(data_dict.get(metric)) - float(ref_result.get(ref_metric))

#                     severity = determine_severity(rule['rule_type'], metric, value, delta)

#                     for action in rule['actions']:
#                         message = compose_message(
#                             action, rule, data_dict, farm_id, source, severity, metric, value,
#                             timestamp, delta, time_offset, data_dict.get('source_count', 4)
#                         )
#                         triggered_actions.append((rule, action, severity, message))

#             resolved_actions = resolve_conflicts(valid_rules, triggered_actions)

#             results = []
#             for rule, action, severity, message in resolved_actions:
#                 stakeholder = rule.get('target_role', 'default')
#                 priority = action['priority']
#                 channels = select_channels(stakeholder, severity, priority, timestamp)
#                 for channel in channels:
#                     endpoint = action.get('callback_url') if channel == 'api_callback' else None
#                     success = send_notification(channel, message, priority, severity, rule['rule_name'], timestamp, endpoint)
#                     if stakeholder == 'science_team':
#                         store_summary_and_notify(farm_id, stakeholder, rule['rule_id'], message, severity, priority)
#                     results.append({
#                         'rule_id': rule['rule_id'],
#                         'action': action['action_type'],
#                         'channel': channel,
#                         'status': 'Success' if success else 'Failed',
#                         'message': message
#                     })

#             return {
#                 'statusCode': 200,
#                 'body': json.dumps({'results': results}, cls=DecimalEncoder)
#             }

#         except Exception as e:
#             logger.error(f"Error processing rules: {str(e)}")
#             return {
#                 'statusCode': 500,
#                 'body': json.dumps({'error': f"Error processing rules: {str(e)}"}, cls=DecimalEncoder)
#             }

#     except Exception as e:
#         logger.error(f"Lambda fatal error: {str(e)}")
#         return {
#             'statusCode': 500,
#             'body': json.dumps({'error': f"Lambda failure: {str(e)}"}, cls=DecimalEncoder)
#         }

#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()
#         logger.info("Database connection closed")


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
