import os
import json
import aiohttp
import asyncio
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# --- LOGGING CONFIG ---
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

# --- LOCATIONS ---
LOCATIONS = [
    {'lat': 24.5854, 'lon': 73.7125, 'farm_id': 'udaipur_farm1'},
    {'lat': 25.1234, 'lon': 74.5678, 'farm_id': 'location2'},
    {'lat': 26.4321, 'lon': 75.8765, 'farm_id': 'location3'}
]

# --- ASYNC API HANDLERS ---

async def fetch_openweather(session, location):
    try:
        current_url = f"https://api.openweathermap.org/data/2.5/weather?lat={location['lat']}&lon={location['lon']}&appid={OPENWEATHER_API_KEY}&units=metric"
        async with session.get(current_url) as response:
            current_data = await response.json()

        current = {
            "temperature_c": current_data['main']['temp'],
            "humidity_percent": current_data['main']['humidity'],
            "wind_speed_mps": current_data['wind']['speed'],
            "wind_direction_deg": current_data['wind'].get('deg'),
            "rainfall_mm": current_data.get('rain', {}).get('1h', 0)
        }

        forecast_url = f"https://api.openweathermap.org/data/2.5/forecast?lat={location['lat']}&lon={location['lon']}&appid={OPENWEATHER_API_KEY}&units=metric"
        async with session.get(forecast_url) as response:
            forecast_data = await response.json()

        forecasts = []
        for item in forecast_data['list']:
            forecasts.append({
                "forecast_for": datetime.fromtimestamp(item['dt'], tz=timezone.utc),
                "temperature_c": item['main']['temp'],
                "humidity_percent": item['main']['humidity'],
                "wind_speed_mps": item['wind']['speed'],
                "wind_direction_deg": item['wind'].get('deg'),
                "rainfall_mm": item.get('rain', {}).get('3h', 0),
                "chance_of_rain_percent": item.get('pop', 0) * 100
            })

        return {"source": "openweather", "current": current, "forecast": forecasts}
    except Exception as e:
        logger.error("OpenWeather fetch error for farm_id=%s: %s", location['farm_id'], str(e), extra={"farm_id": location['farm_id']})
        return {"error": f"OpenWeather fetch error: {str(e)}", "farm_id": location['farm_id']}

async def fetch_weatherapi(session, location):
    try:
        url = f"http://api.weatherapi.com/v1/forecast.json?key={WEATHERAPI_API_KEY}&q={location['lat']},{location['lon']}&days=5&aqi=no&alerts=no"
        async with session.get(url) as response:
            data = await response.json()

        current_data = {
            "temperature_c": data['current']['temp_c'],
            "humidity_percent": data['current']['humidity'],
            "wind_speed_mps": data['current']['wind_kph'] / 3.6,
            "wind_direction_deg": data['current']['wind_degree'],
            "rainfall_mm": data['current'].get('precip_mm', 0)
        }

        forecasts = []
        for day in data['forecast']['forecastday']:
            for hour in day['hour']:
                forecasts.append({
                    "forecast_for": datetime.strptime(hour['time'], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc),
                    "temperature_c": hour['temp_c'],
                    "humidity_percent": hour['humidity'],
                    "wind_speed_mps": hour['wind_kph'] / 3.6,
                    "wind_direction_deg": hour['wind_degree'],
                    "rainfall_mm": hour.get('precip_mm', 0),
                    "chance_of_rain_percent": hour.get('chance_of_rain')
                })

        return {"source": "weatherapi", "current": current_data, "forecast": forecasts}
    except Exception as e:
        logger.error("WeatherAPI fetch error for farm_id=%s: %s", location['farm_id'], str(e), extra={"farm_id": location['farm_id']})
        return {"error": f"WeatherAPI fetch error: {str(e)}", "farm_id": location['farm_id']}

async def fetch_yrno(session, location):
    try:
        url = f"https://api.met.no/weatherapi/locationforecast/2.0/compact?lat={location['lat']}&lon={location['lon']}"
        headers = {"User-Agent": "WeatherFetcher/1.0"}
        async with session.get(url, headers=headers) as response:
            data = await response.json()

        now_data = data['properties']['timeseries'][0]['data']['instant']['details']
        current_data = {
            "temperature_c": now_data['air_temperature'],
            "humidity_percent": now_data.get('relative_humidity'),
            "wind_speed_mps": now_data['wind_speed'],
            "wind_direction_deg": now_data['wind_from_direction'],
            "rainfall_mm": 0
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

        return {"source": "yrno", "current": current_data, "forecast": forecasts}
    except Exception as e:
        logger.error("Yr.no fetch error for farm_id=%s: %s", location['farm_id'], str(e), extra={"farm_id": location['farm_id']})
        return {"error": f"Yr.no fetch error: {str(e)}", "farm_id": location['farm_id']}

async def fetch_openmeteo(session, location):
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={location['lat']}&longitude={location['lon']}&current=temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,precipitation&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,precipitation&forecast_days=5"
        async with session.get(url) as response:
            data = await response.json()

        current_data = {
            "temperature_c": data['current'].get('temperature_2m'),
            "humidity_percent": data['current'].get('relative_humidity_2m'),
            "wind_speed_mps": data['current'].get('wind_speed_10m'),
            "wind_direction_deg": data['current'].get('wind_direction_10m'),
            "rainfall_mm": data['current'].get('precipitation')
        }

        forecasts = []
        for i, t in enumerate(data['hourly']['time']):
            ts = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
            if (ts - datetime.now(timezone.utc)).days > 5:
                break
            forecasts.append({
                "forecast_for": ts,
                "temperature_c": data['hourly']['temperature_2m'][i],
                "humidity_percent": data['hourly']['relative_humidity_2m'][i],
                "wind_speed_mps": data['hourly']['wind_speed_10m'][i],
                "wind_direction_deg": data['hourly']['wind_direction_10m'][i],
                "rainfall_mm": data['hourly']['precipitation'][i],
                "chance_of_rain_percent": None
            })

        return {"source": "openmeteo", "current": current_data, "forecast": forecasts}
    except Exception as e:
        logger.error("Open-Meteo fetch error for farm_id=%s: %s", location['farm_id'], str(e), extra={"farm_id": location['farm_id']})
        return {"error": f"Open-Meteo fetch error: {str(e)}", "farm_id": location['farm_id']}

# --- DB INSERTS ---

def insert_current_weather(conn, cursor, source, farm_id, location, data, timestamp):
    cursor.execute("""
        INSERT INTO current_weather (
            source, farm_id, location, timestamp,
            temperature_c, humidity_percent, wind_speed_mps,
            wind_direction_deg, rainfall_mm
        )
        VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s,
                %s, %s, %s, %s, %s)
        ON CONFLICT (farm_id, source, timestamp)
        DO UPDATE SET
            temperature_c = EXCLUDED.temperature_c,
            humidity_percent = EXCLUDED.humidity_percent,
            wind_speed_mps = EXCLUDED.wind_speed_mps,
            wind_direction_deg = EXCLUDED.wind_direction_deg,
            rainfall_mm = EXCLUDED.rainfall_mm
    """, (
        source, farm_id, location['lon'], location['lat'], timestamp,
        data['temperature_c'], data['humidity_percent'], data['wind_speed_mps'],
        data['wind_direction_deg'], data['rainfall_mm']
    ))

def insert_forecast_weather(conn, cursor, source, farm_id, location, data, fetched_at):
    for forecast in data:
        cursor.execute("""
            INSERT INTO forecast_weather (
                source, farm_id, location, forecast_for, fetched_at,
                temperature_c, humidity_percent, wind_speed_mps,
                wind_direction_deg, rainfall_mm, chance_of_rain_percent
            )
            VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s,
                    %s, %s, %s, %s, %s, %s)
            ON CONFLICT (farm_id, source, forecast_for)
            DO UPDATE SET
                fetched_at = EXCLUDED.fetched_at,
                temperature_c = EXCLUDED.temperature_c,
                humidity_percent = EXCLUDED.humidity_percent,
                wind_speed_mps = EXCLUDED.wind_speed_mps,
                wind_direction_deg = EXCLUDED.wind_direction_deg,
                rainfall_mm = EXCLUDED.rainfall_mm,
                chance_of_rain_percent = EXCLUDED.chance_of_rain_percent
        """, (
            source, farm_id, location['lon'], location['lat'], forecast['forecast_for'], fetched_at,
            forecast['temperature_c'], forecast['humidity_percent'], forecast['wind_speed_mps'],
            forecast['wind_direction_deg'], forecast['rainfall_mm'], forecast.get('chance_of_rain_percent')
        ))

# --- ASYNC FETCHER ---

async def fetch_all_weather(locations):
    fetchers = [fetch_openweather, fetch_weatherapi, fetch_yrno, fetch_openmeteo]
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetcher(session, loc) for loc in locations for fetcher in fetchers
        ]
        return await asyncio.gather(*tasks, return_exceptions=False)

# --- MAIN HANDLER ---

def lambda_handler(event, context):
    conn = cursor = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS,
            host=DB_HOST, port=DB_PORT, cursor_factory=RealDictCursor
        )
        cursor = conn.cursor()
        logger.info("DB connection successful")

        # Run async API fetches
        loop = asyncio.get_event_loop()
        results = loop.run_until_complete(fetch_all_weather(LOCATIONS))
        
        timestamp = datetime.now(timezone.utc)
        errors = []

        # Process results
        for loc in LOCATIONS:
            for result in [r for r in results if 'farm_id' in r and r['farm_id'] == loc['farm_id'] or 'farm_id' not in r]:
                if 'error' in result:
                    errors.append(result['error'])
                    continue
                try:
                    insert_current_weather(conn, cursor, result['source'], loc['farm_id'], loc, result['current'], timestamp)
                    insert_forecast_weather(conn, cursor, result['source'], loc['farm_id'], loc, result['forecast'], timestamp)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    errors.append(f"DB insert error for {loc['farm_id']}, source={result['source']}: {str(e)}")

        return {
            'statusCode': 200 if not errors else 500,
            'body': json.dumps({
                'message': 'Completed with errors' if errors else 'Success',
                'errors': errors,
                'timestamp': timestamp.isoformat()
            })
        }
    except Exception as e:
        logger.exception("Lambda fatal error")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Lambda failure',
                'error': str(e)
            })
        }
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
