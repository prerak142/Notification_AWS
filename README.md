# Weather Rule Engine System

This repository implements a modular, serverless system for weather-based rule evaluation and alert generation using AWS services. The system ingests weather data from multiple APIs, allows custom rule submissions via an API, and triggers stakeholder alerts based on real-time weather conditions.

This `README.md` serves as a complete handover document, including implementation details, architecture, setup steps, usage instructions, and API specifications.

---

## Directory Structure

```
lambda/
├── RuleEntryLambda/
│   └── src/
│       └── lambda_function.py        # Accepts and stores rule definitions in DynamoDB
├── RulesEngine/
│   └── src/
│       └── lambda_function.py        # Evaluates rules, triggers alerts and reports
└── WeatherDataIngestion/
    └── src/
        └── lambda_function.py        # Fetches weather data and stores in PostgreSQL

```

---

## System Components

### 1. Weather Data Ingestion Lambda

- Fetches data from:
  - Yr.no
  - Open-Meteo
  - OpenWeatherMap
  - WeatherAPI
- Stores data in AWS RDS PostgreSQL:
  - `weather_current`
  - `weather_forecast`
- Uses majority-voting across APIs for robustness.
- Runs on a schedule (e.g., every 30 minutes).

### 2. Rule Entry Lambda + API Gateway

- Accepts POST requests with rule definitions in JSON format.
- Stores them in the `Rules` DynamoDB table.
- API Gateway exposes a REST endpoint with CORS enabled.
- A minimal frontend (`UI/index.html`) allows rule submission by stakeholders or admins.

### 3. Rules Engine Lambda

- Triggered by CloudWatch (cron), SQS, or test events.
- Retrieves rules by `farm_id` and `stakeholder`.
- Fetches weather data from RDS.
- Evaluates conditions: Simple, Time-based, Delta, Sequential
- Sends:
  - SMS/Email alerts via SNS
  - Science team updates via SQS
  - Blog/report files to S3

---

## Setup Instructions

### Prerequisites

- AWS Account (Free Tier)
- PostgreSQL RDS (with schema)
- API Keys:
  - OpenWeatherMap
  - WeatherAPI

---

### Step 1: RDS Setup

Provision a PostgreSQL DB:

```sql
CREATE TABLE weather_forecast (
  id SERIAL PRIMARY KEY,
  farm_id VARCHAR(50),
  timestamp TIMESTAMP,
  temperature_c FLOAT,
  humidity_percent FLOAT,
  wind_speed_mps FLOAT,
  wind_direction_deg FLOAT,
  rainfall_mm FLOAT,
  chance_of_rain_percent FLOAT,
  source VARCHAR(50),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE weather_current (
  id SERIAL PRIMARY KEY,
  farm_id VARCHAR(50),
  timestamp TIMESTAMP,
  temperature_c FLOAT,
  humidity_percent FLOAT,
  wind_speed_mps FLOAT,
  wind_direction_deg FLOAT,
  rainfall_mm FLOAT,
  solar_radiation_wm2 FLOAT,
  source VARCHAR(50),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

### Step 2: Deploy Lambdas

Deploy each lambda with:
- Runtime: Python 3.12
- Architecture: x86_64
- Timeout: 1 min
- Memory: 128MB

Add layers (as needed):
- psycopg2 for PostgreSQL
- pandas for data manipulation

Set required environment variables:

```
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS
OPENWEATHER_API_KEY, WEATHERAPI_API_KEY
SNS_TOPIC_ARN, SQS_QUEUE_URL, S3_BUCKET_NAME
```

---

## Testing

- Use Lambda Console or `curl` to test ingestion.
- Use HTML form in `UI/` to submit rules.
- Insert dummy data in PostgreSQL and trigger RulesEngine manually.
- Check CloudWatch logs, S3, and DynamoDB to verify functionality.

---

## API Documentation

### Rule Entry API (Swagger Style)

```
POST /store-rule
```

#### Description:
Submit a new rule to DynamoDB.

#### Request Headers:
```http
Content-Type: application/json
```

#### Request Body (JSON):
```json
{
  "rule_id": "rule123",
  "rule_name": "HighTemperatureAlert",
  "rule_type": "weather",
  "data_type": "current",
  "farm_id": "udaipur_farm1",
  "stakeholder": "farmer",
  "language": "en",
  "conditions": {
    "metric": "temperature_c",
    "operator": ">",
    "value": 30
  },
  "actions": [
    {
      "type": "sms",
      "scenario": "high_temperature",
      "message": "Urgent: High temperature detected."
    }
  ],
  "stop_on_match": true,
  "conflict_resolution": "first_match",
  "priority": 1,
  "active": true
}
```

#### Response
```json
{
  "message": "Rule stored successfully",
  "rule_id": "rule123"
}
```

---

## Rule Evaluation Logic

### Supported Condition Types:
- Simple: `metric operator value`
- Time-based: `TIME_WINDOW`, `DAY_DIFF`, etc.
- Delta: `delta_gt`, `delta_lt`
- Sequential: `SEQUENCE`

### Conflict Resolution Strategies:
- `first_match`
- `highest_priority`
- `most_severe`

---

## IAM Roles Required

Each Lambda should have:
- `AWSLambdaBasicExecutionRole`
- `AmazonRDSFullAccess` or scoped RDS permissions
- `AmazonDynamoDBFullAccess` or scoped table access
- `AmazonSNSFullAccess`, `AmazonSQSFullAccess`
- `AmazonS3FullAccess` (for blog uploads)

---

## Maintenance Notes

- **Logging**: Use CloudWatch for Lambda and API Gateway logs.
- **API Keys**: Rotate OpenWeatherMap and WeatherAPI keys regularly.
- **Security**: Use VPC for RDS and scoped IAM roles.
- **Backup**: Enable point-in-time recovery for DynamoDB and snapshotting for RDS.

---

## Future Improvements

- Swagger/OpenAPI 3.0 spec for `/store-rule`
- Rule listing, editing, and deleting from UI
- Integration with Grafana or QuickSight for dashboards
- ML-based forecasting/alerting

---

## License

This project is licensed under the MIT License.
