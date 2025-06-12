import os
import json
import requests
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import RealDictCursor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

# --- SETUP LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- ENV CONFIG ---
OPENWEATHER_API_KEY = os.environ.get('OPENWEATHER_API_KEY')
WEATHERAPI_API_KEY = os.environ.get('WEATHERAPI_API_KEY')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')

# Validate environment variables
required_env_vars = ['OPENWEATHER_API_KEY', 'WEATHERAPI_API_KEY', 'DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASS']
missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
if missing_vars:
    raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

# --- RETRY SESSION ---
session = requests.Session()
session.timeout = 10  # 10-second timeout for API calls
retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
session.mount('http://', HTTPAdapter(max_retries=retries))
session.mount('https://', HTTPAdapter(max_retries=retries))

# --- LOCATIONS ---
LOCATIONS = [
    {'lat': 24.5854, 'lon': 73.7125, 'farm_id': 'udaipur_farm1'},
    {'lat': 25.1234, 'lon': 74.5678, 'farm_id': 'location2'},
    {'lat': 26.4321, 'lon': 75.8765, 'farm_id': 'location3'}
]

# --- SENSOR FALLBACK ---
def fetch_sensor_fallback(conn, cursor, farm_id, timestamp):
    """Fetch historical averages as a fallback for missing API data."""
    try:
        cursor.execute("""
            SELECT AVG(temperature_c) as temperature_c,
                   AVG(humidity_percent) as humidity_percent,
                   AVG(wind_speed_mps) as wind_speed_mps,
                   AVG(wind_direction_deg) as wind_direction_deg,
                   AVG(rainfall_mm) as rainfall_mm
            FROM current_weather
            WHERE farm_id = %s
            AND timestamp >= %s - INTERVAL '7 days'
        """, (farm_id, timestamp))
        result = cursor.fetchone()
        if result and result['temperature_c'] is not None:
            return {
                "source": "sensor_fallback",
                "current": {
                    "temperature_c": result['temperature_c'],
                    "humidity_percent": result['humidity_percent'],
                    "wind_speed_mps": result['wind_speed_mps'],
                    "wind_direction_deg": result['wind_direction_deg'],
                    "rainfall_mm": result['rainfall_mm'],
                    "solar_radiation_wm2": None
                },
                "forecast": []  # No forecast data for sensor fallback
            }
        return None
    except Exception as e:
        logger.error(f"Sensor fallback failed for {farm_id}: {str(e)}")
        return None

# --- API HANDLERS ---

def fetch_openweather(location):
    try:
        current_url = f"https://api.openweathermap.org/data/2.5/weather?lat={location['lat']}&lon={location['lon']}&appid={OPENWEATHER_API_KEY}&units=metric"
        response = session.get(current_url)
        response.raise_for_status()
        current_data = response.json()
        if 'main' not in current_data:
            raise ValueError("Invalid OpenWeather response")

        current = {
            "temperature_c": current_data['main']['temp'],
            "humidity_percent": current_data['main']['humidity'],
            "wind_speed_mps": current_data['wind']['speed'],
            "wind_direction_deg": current_data['wind'].get('deg'),
            "rainfall_mm": current_data.get('rain', {}).get('1h', 0),
            "solar_radiation_wm2": None
        }

        forecast_url = f"https://api.openweathermap.org/data/2.5/forecast?lat={location['lat']}&lon={location['lon']}&appid={OPENWEATHER_API_KEY}&units=metric"
        response = session.get(forecast_url)
        response.raise_for_status()
        forecast_data = response.json()
        forecasts = []
        for item in forecast_data['list']:
            forecast_time = datetime.fromtimestamp(item['dt'], tz=timezone.utc)
            if (forecast_time - datetime.now(timezone.utc)).days > 5:
                break
            forecasts.append({
                "forecast_for": forecast_time,
                "temperature_c": item['main']['temp'],
                "humidity_percent": item['main']['humidity'],
                "wind_speed_mps": item['wind']['speed'],
                "wind_direction_deg": item['wind'].get('deg'),
                "rainfall_mm": item.get('rain', {}).get('3h', 0),
                "chance_of_rain_percent": item.get('pop', 0) * 100
            })

        return {
            "source": "openweather",
            "current": current,
            "forecast": forecasts
        }
    except Exception as e:
        logger.error(f"OpenWeather fetch failed: {str(e)}")
        return None

def fetch_weatherapi(location):
    try:
        forecast_url = f"http://api.weatherapi.com/v1/forecast.json?key={WEATHERAPI_API_KEY}&q={location['lat']},{location['lon']}&days=5&aqi=no&alerts=no"
        response = session.get(forecast_url)
        response.raise_for_status()
        data = response.json()
        if 'current' not in data:
            raise ValueError("Invalid WeatherAPI response")

        current = data['current']
        current_data = {
            "temperature_c": current['temp_c'],
            "humidity_percent": current['humidity'],
            "wind_speed_mps": current['wind_kph'] / 3.6,
            "wind_direction_deg": current['wind_degree'],
            "rainfall_mm": current.get('precip_mm', 0),
            "solar_radiation_wm2": None
        }

        forecasts = []
        for day in data['forecast']['forecastday']:
            for hour in day['hour']:
                forecast_time = datetime.strptime(hour['time'], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
                if (forecast_time - datetime.now(timezone.utc)).days > 5:
                    break
                forecasts.append({
                    "forecast_for": forecast_time,
                    "temperature_c": hour['temp_c'],
                    "humidity_percent": hour['humidity'],
                    "wind_speed_mps": hour['wind_kph'] / 3.6,
                    "wind_direction_deg": hour['wind_degree'],
                    "rainfall_mm": hour['precip_mm'],
                    "chance_of_rain_percent": hour.get('chance_of_rain', None)
                })

        return {
            "source": "weatherapi",
            "current": current_data,
            "forecast": forecasts
        }
    except Exception as e:
        logger.error(f"WeatherAPI fetch failed: {str(e)}")
        return None

def fetch_yrno(location):
    try:
        url = f"https://api.met.no/weatherapi/locationforecast/2.0/compact?lat={location['lat']}&lon={location['lon']}"
        headers = {"User-Agent": "WeatherFetcher/1.0"}
        response = session.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        if 'properties' not in data:
            raise ValueError("Invalid Yr.no response")

        now_data = data['properties']['timeseries'][0]['data']['instant']['details']
        current_data = {
            "temperature_c": now_data['air_temperature'],
            "humidity_percent": now_data.get('relative_humidity'),
            "wind_speed_mps": now_data['wind_speed'],
            "wind_direction_deg": now_data['wind_from_direction'],
            "rainfall_mm": 0,
            "solar_radiation_wm2": None
        }

        forecasts = []
        for item in data['properties']['timeseries']:
            ts = datetime.strptime(item['time'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            if (ts - datetime.now(timezone.utc)).days > 5:
                break
            inst = item['data']['instant']['details']
            forecasts.append({
                "forecast_for": ts,
                "temperature_c": inst.get('air_temperature'),
                "humidity_percent": inst.get('relative_humidity'),
                "wind_speed_mps": inst.get('wind_speed'),
                "wind_direction_deg": inst.get('wind_from_direction'),
                "rainfall_mm": item['data'].get('next_1_hours', {}).get('details', {}).get('precipitation_amount', 0),
                "chance_of_rain_percent": None
            })

        return {
            "source": "yrno",
            "current": current_data,
            "forecast": forecasts
        }
    except Exception as e:
        logger.error(f"Yr.no fetch failed: {str(e)}")
        return None

def fetch_openmeteo(location):
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={location['lat']}&longitude={location['lon']}&current=temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,precipitation,direct_radiation&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,precipitation,direct_radiation&forecast_days=5"
        response = session.get(url)
        response.raise_for_status()
        data = response.json()
        if 'current' not in data:
            raise ValueError("Invalid Open-Meteo response")

        current = data['current']
        current_data = {
            "temperature_c": current.get('temperature_2m'),
            "humidity_percent": current.get('relative_humidity_2m'),
            "wind_speed_mps": current.get('wind_speed_10m'),
            "wind_direction_deg": current.get('wind_direction_10m'),
            "rainfall_mm": current.get('precipitation'),
            "solar_radiation_wm2": current.get('direct_radiation')
        }

        forecasts = []
        for i in range(len(data['hourly']['time'])):
            forecast_time = datetime.fromisoformat(data['hourly']['time'][i]).replace(tzinfo=timezone.utc)
            if (forecast_time - datetime.now(timezone.utc)).days > 5:
                break
            forecasts.append({
                "forecast_for": forecast_time,
                "temperature_c": data['hourly']['temperature_2m'][i],
                "humidity_percent": data['hourly']['relative_humidity_2m'][i],
                "wind_speed_mps": data['hourly']['wind_speed_10m'][i],
                "wind_direction_deg": data['hourly']['wind_direction_10m'][i],
                "rainfall_mm": data['hourly']['precipitation'][i],
                "chance_of_rain_percent": None,
                "solar_radiation_wm2": data['hourly']['direct_radiation'][i]
            })

        return {
            "source": "openmeteo",
            "current": current_data,
            "forecast": forecasts
        }
    except Exception as e:
        logger.error(f"Open-Meteo fetch failed: {str(e)}")
        return None

# --- DB INSERTS ---

def insert_current_weather(conn, cursor, source, farm_id, location, data, timestamp):
    try:
        cursor.execute("""
            INSERT INTO current_weather (
                source, farm_id, location, timestamp,
                temperature_c, humidity_percent, wind_speed_mps,
                wind_direction_deg, rainfall_mm, solar_radiation_wm2
            )
            VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s,
                    %s, %s, %s, %s, %s, %s)
            ON CONFLICT (farm_id, source, timestamp)
            DO UPDATE SET
                location = EXCLUDED.location,
                temperature_c = EXCLUDED.temperature_c,
                humidity_percent = EXCLUDED.humidity_percent,
                wind_speed_mps = EXCLUDED.wind_speed_mps,
                wind_direction_deg = EXCLUDED.wind_direction_deg,
                rainfall_mm = EXCLUDED.rainfall_mm,
                solar_radiation_wm2 = EXCLUDED.solar_radiation_wm2
        """, (
            source, farm_id, location['lon'], location['lat'], timestamp,
            data['temperature_c'], data['humidity_percent'], data['wind_speed_mps'],
            data['wind_direction_deg'], data['rainfall_mm'], data['solar_radiation_wm2']
        ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert current weather for {farm_id} from {source}: {str(e)}")
        raise e

def insert_forecast_weather(conn, cursor, source, farm_id, location, data, fetched_at):
    try:
        for forecast in data:
            cursor.execute("""
                INSERT INTO forecast_weather (
                    source, farm_id, location, forecast_for, fetched_at,
                    temperature_c, humidity_percent, wind_speed_mps,
                    wind_direction_deg, rainfall_mm, chance_of_rain_percent, solar_radiation_wm2
                )
                VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s,
                        %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (farm_id, source, forecast_for)
                DO UPDATE SET
                    location = EXCLUDED.location,
                    fetched_at = EXCLUDED.fetched_at,
                    temperature_c = EXCLUDED.temperature_c,
                    humidity_percent = EXCLUDED.humidity_percent,
                    wind_speed_mps = EXCLUDED.wind_speed_mps,
                    wind_direction_deg = EXCLUDED.wind_direction_deg,
                    rainfall_mm = EXCLUDED.rainfall_mm,
                    chance_of_rain_percent = EXCLUDED.chance_of_rain_percent,
                    solar_radiation_wm2 = EXCLUDED.solar_radiation_wm2
            """, (
                source, farm_id, location['lon'], location['lat'], forecast['forecast_for'], fetched_at,
                forecast['temperature_c'], forecast['humidity_percent'], forecast.get('wind_speed_mps'),
                forecast.get('wind_direction_deg'), forecast['rainfall_mm'],
                forecast.get('chance_of_rain_percent'), forecast.get('solar_radiation_wm2')
            ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert forecast weather for {farm_id} from {source}: {str(e)}")
        raise e

# --- GEOSPATIAL MAPPING ---
def update_farm_locations(conn, cursor, locations):
    """Ensure farm locations are stored in a farms table for geospatial queries."""
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS farms (
                farm_id VARCHAR PRIMARY KEY,
                location GEOGRAPHY(Point, 4326)
            )
        """)
        for location in locations:
            cursor.execute("""
                INSERT INTO farms (farm_id, location)
                VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                ON CONFLICT (farm_id)
                DO UPDATE SET location = EXCLUDED.location
            """, (location['farm_id'], location['lon'], location['lat']))
        conn.commit()
        logger.info("Farm locations updated successfully")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to update farm locations: {str(e)}")
        raise e

def get_nearby_farms(conn, cursor, lat, lon, radius_km=10):
    """Query farms within a radius for geospatial analysis."""
    try:
        cursor.execute("""
            SELECT farm_id
            FROM farms
            WHERE ST_DWithin(
                location,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s
            )
        """, (lon, lat, radius_km * 1000))  # Convert km to meters
        return [row['farm_id'] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Failed to query nearby farms: {str(e)}")
        return []

# --- MAIN LAMBDA HANDLER ---

def lambda_handler(event, context):
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        host=DB_HOST, port=DB_PORT, cursor_factory=RealDictCursor,
        connect_timeout=5
    )
    cursor = conn.cursor()
    timestamp = datetime.now(timezone.utc)
    errors = []

    try:
        # Update farm locations in the database
        update_farm_locations(conn, cursor, LOCATIONS)

        # Define API fetchers in priority order
        fetchers = [fetch_openweather, fetch_weatherapi, fetch_yrno, fetch_openmeteo]

        for location in LOCATIONS:
            farm_id = location['farm_id']
            data = None
            # Try APIs in order until one succeeds
            for fetcher in fetchers:
                try:
                    data = fetcher(location)
                    if data:
                        logger.info(f"Successfully fetched data for {farm_id} from {data['source']}")
                        break
                except Exception as e:
                    logger.warning(f"Failed {fetcher.__name__} for {farm_id}: {str(e)}")
                    continue

            # If all APIs fail, try sensor fallback
            if not data:
                logger.warning(f"All APIs failed for {farm_id}, attempting sensor fallback")
                data = fetch_sensor_fallback(conn, cursor, farm_id, timestamp)
                if not data:
                    errors.append(f"No data available for {farm_id}")
                    continue

            # Insert data into database
            try:
                insert_current_weather(conn, cursor, data['source'], farm_id, location, data['current'], timestamp)
                if data['forecast']:  # Only insert forecast if available
                    insert_forecast_weather(conn, cursor, data['source'], farm_id, location, data['forecast'], timestamp)
                logger.info(f"Data inserted for {farm_id} from {data['source']}")
            except Exception as e:
                errors.append(f"Database insert failed for {farm_id} from {data['source']}: {str(e)}")

        # Example geospatial query: Find nearby farms
        for location in LOCATIONS:
            nearby_farms = get_nearby_farms(conn, cursor, location['lat'], location['lon'], radius_km=10)
            logger.info(f"Nearby farms to {location['farm_id']}: {nearby_farms}")

        if errors:
            return {
                'statusCode': 500,
                'body': json.dumps({'message': 'Some data ingestion failed', 'errors': errors})
            }
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Weather data ingested successfully'})
        }
    finally:
        cursor.close()
        conn.close()
