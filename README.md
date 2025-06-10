# Climate Resilient Farms of the Future: Notification Rules Engine

This project enables real-time weather monitoring and rule-based notifications for climate-smart farming. It integrates data from multiple weather APIs, stores it in AWS-backed infrastructure, and triggers alerts based on user-defined conditions.

## Weather Data Sources
- OpenWeather
- WeatherAPI
- Yr.no
- Open-Meteo

## AWS Lambda Functions
- **WeatherDataIngestion**: Pulls weather data from APIs and stores it in PostgreSQL.
- **RulesEngine**: Checks weather data against user-defined rules from DynamoDB and sends alerts.
- **RuleApiLambda**: REST API to manage rules via API Gateway.

## AWS Infrastructure
- **PostgreSQL (RDS)**: Stores `current_weather` and `forecast_weather` tables.
- **DynamoDB**: Stores rule definitions in `WeatherRules` table.
- **SNS**: Sends email/SMS alerts. Topic ARN: `arn:aws:sns:ap-south-1:580075786360:weather-alerts`
- **API Gateway**: Endpoint: `https://9qzfpocell.execute-api.ap-south-1.amazonaws.com/prod/rules`


## Database Overview

### PostgreSQL Tables

#### `current_weather`
- **Fields**: `source`, `farm_id`, `location`, `timestamp`, `temperature_c`, `humidity_percent`, `wind_speed_mps`, `wind_direction_deg`, `rainfall_mm`, `solar_radiation_wm2`
- **Indexed on**: `farm_id`, `timestamp`, `location` (GIST), `(source, timestamp)`
- **Unique**: `(farm_id, source, timestamp)`

#### `forecast_weather`
- **Fields**: `source`, `farm_id`, `forecast_for`, `fetched_at`, `location`, `temperature_c`, `humidity_percent`, `wind_speed_mps`, `wind_direction_deg`, `rainfall_mm`, `chance_of_rain_percent`
- **Indexed on**: `farm_id`, `forecast_for`, `location` (GIST), `(source, fetched_at)`
- **Unique**: `(farm_id, source, forecast_for)`

### DynamoDB Table: `WeatherRules`
- **Partition Key**: `farm_id`
- **Sort Key**: `stakeholder`
- **Attributes**: `rule_id`, `name`, `priority`, `data_type`, `conditions`, `actions`, `stop_on_match`
- **GSI**: `StakeholderIndex` on `(farm_id, stakeholder)`

## ⚙️ Configuration

### Required Environment Variables

#### WeatherDataIngestion
- `OPENWEATHER_API_KEY`
- `WEATHERAPI_API_KEY`
- `OPEN_METEO_URL`
- `YR_NO_URL`
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASS`
- `SNS_TOPIC_ARN`

1
AWSSDKPandas-Python312
16
python3.12
x86_64
arn:aws:lambda:ap-south-1:336392948345:layer:AWSSDKPandas-Python312:16
2
Klayers-p312-psycopg2-binary
1
python3.12
x86_64
arn:aws:lambda:ap-south-1:770693421928:layer:Klayers-p312-psycopg2-binary:1

#### RulesEngine
- Same DB credentials as above
- `API_KEY`, `RULE_ID`

#### RuleApiLambda
- No additional variables

## Deployment Instructions

### Prerequisites
- AWS CLI & SAM CLI
- Python 3.12
- IAM roles with Lambda, RDS, DynamoDB, SNS, and API Gateway permissions

1
AWSSDKPandas-Python312
16
python3.12
x86_64
arn:aws:lambda:ap-south-1:336392948345:layer:AWSSDKPandas-Python312:16
2
Klayers-p312-psycopg2-binary
1
python3.12
x86_64
arn:aws:lambda:ap-south-1:770693421928:layer:Klayers-p312-psycopg2-binary:1
## Database Overview

### PostgreSQL Tables

#### `current_weather`
- **Fields**: `source`, `farm_id`, `location`, `timestamp`, `temperature_c`, `humidity_percent`, `wind_speed_mps`, `wind_direction_deg`, `rainfall_mm`, `solar_radiation_wm2`
- **Indexed on**: `farm_id`, `timestamp`, `location` (GIST), `(source, timestamp)`
- **Unique**: `(farm_id, source, timestamp)`

#### `forecast_weather`
- **Fields**: `source`, `farm_id`, `forecast_for`, `fetched_at`, `location`, `temperature_c`, `humidity_percent`, `wind_speed_mps`, `wind_direction_deg`, `rainfall_mm`, `chance_of_rain_percent`
- **Indexed on**: `farm_id`, `forecast_for`, `location` (GIST), `(source, fetched_at)`
- **Unique**: `(farm_id, source, forecast_for)`

### DynamoDB Table: `WeatherRules`
- **Partition Key**: `farm_id`
- **Sort Key**: `stakeholder`
- **Attributes**: `rule_id`, `name`, `priority`, `data_type`, `conditions`, `actions`, `stop_on_match`
- **GSI**: `StakeholderIndex` on `(farm_id, stakeholder)`

## ⚙️ Configuration

### Required Environment Variables

#### WeatherDataIngestion
- `OPENWEATHER_API_KEY`
- `WEATHERAPI_API_KEY`
- `OPEN_METEO_URL`
- `YR_NO_URL`
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASS`
- `SNS_TOPIC_ARN`

#### RulesEngine
- Same DB credentials as above
- `API_KEY`, `RULE_ID`

#### RuleApiLambda
- No additional variables

## Deployment Instructions

### Prerequisites
- AWS CLI & SAM CLI
- Python 3.12
- IAM roles with Lambda, RDS, DynamoDB, SNS, and API Gateway permissions



