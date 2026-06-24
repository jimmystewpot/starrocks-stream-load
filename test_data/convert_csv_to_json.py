import csv
import json
from datetime import datetime
import os
import gzip

def open_input_file(filepath):
    if os.path.exists(filepath):
        return open(filepath, mode='r', encoding='utf-8')
    elif os.path.exists(filepath + ".gz"):
        return gzip.open(filepath + ".gz", mode='rt', encoding='utf-8')
    else:
        raise FileNotFoundError(f"Neither {filepath} nor {filepath}.gz exists")

def convert_weather_data():
    csv_path = "test_data/72505394728.csv"
    json_path = "test_data/weatherdata.json"
    
    print(f"Converting {csv_path} to {json_path}...")
    records = []
    
    with open_input_file(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parse Date: e.g. "2014-01-01T00:51:00"
            date_str = row.get("DATE", "")
            if date_str:
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
                    formatted_date = dt.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    formatted_date = date_str
            else:
                formatted_date = None
                
            record = {
                "DATE": formatted_date,
                "NAME": row.get("NAME"),
                "HourlyDewPointTemperature": row.get("HourlyDewPointTemperature"),
                "HourlyDryBulbTemperature": row.get("HourlyDryBulbTemperature"),
                "HourlyPrecipitation": row.get("HourlyPrecipitation"),
                "HourlyPresentWeatherType": row.get("HourlyPresentWeatherType"),
                "HourlyPressureChange": row.get("HourlyPressureChange"),
                "HourlyPressureTendency": row.get("HourlyPressureTendency"),
                "HourlyRelativeHumidity": row.get("HourlyRelativeHumidity"),
                "HourlySkyConditions": row.get("HourlySkyConditions"),
                "HourlyVisibility": row.get("HourlyVisibility"),
                "HourlyWetBulbTemperature": row.get("HourlyWetBulbTemperature"),
                "HourlyWindDirection": row.get("HourlyWindDirection"),
                "HourlyWindGustSpeed": row.get("HourlyWindGustSpeed"),
                "HourlyWindSpeed": row.get("HourlyWindSpeed")
            }
            records.append(record)
            
    with open(json_path, mode='w', encoding='utf-8') as f:
        json.dump(records, f, indent=2)
    print(f"Successfully converted {len(records)} records for weatherdata.")

def convert_crash_data():
    csv_path = "test_data/NYPD_Crash_Data.csv"
    json_path = "test_data/crashdata.json"
    
    print(f"Converting {csv_path} to {json_path}...")
    records = []
    
    with open_input_file(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parse Date & Time: "CRASH DATE" (e.g. 08/05/2014) and "CRASH TIME" (e.g. 9:10)
            date_str = row.get("CRASH DATE", "")
            time_str = row.get("CRASH TIME", "")
            formatted_date = None
            if date_str and time_str:
                # Time could be H:M or HH:MM
                for fmt in ("%m/%d/%Y %H:%M", "%m/%d/%Y %k:%M"):
                    try:
                        dt = datetime.strptime(f"{date_str} {time_str}", fmt)
                        formatted_date = dt.strftime("%Y-%m-%d %H:%M:%S")
                        break
                    except ValueError:
                        continue
                if not formatted_date:
                    formatted_date = f"{date_str} {time_str}"
            
            # Convert numeric types
            def to_int(val):
                if not val:
                    return None
                try:
                    # Latitude/longitude can have decimal points, check if float first
                    return int(float(val))
                except ValueError:
                    return None
            
            record = {
                "CRASH_DATE": formatted_date,
                "BOROUGH": row.get("BOROUGH"),
                "ZIP_CODE": row.get("ZIP CODE"),
                "LATITUDE": to_int(row.get("LATITUDE")),
                "LONGITUDE": to_int(row.get("LONGITUDE")),
                "LOCATION": row.get("LOCATION"),
                "ON_STREET_NAME": row.get("ON STREET NAME"),
                "CROSS_STREET_NAME": row.get("CROSS STREET NAME"),
                "OFF_STREET_NAME": row.get("OFF STREET NAME"),
                "CONTRIBUTING_FACTOR_VEHICLE_1": row.get("CONTRIBUTING FACTOR VEHICLE 1"),
                "CONTRIBUTING_FACTOR_VEHICLE_2": row.get("CONTRIBUTING FACTOR VEHICLE 2"),
                "COLLISION_ID": to_int(row.get("COLLISION ID") or row.get("COLLISION_ID")),
                "VEHICLE_TYPE_CODE_1": row.get("VEHICLE TYPE CODE 1"),
                "VEHICLE_TYPE_CODE_2": row.get("VEHICLE TYPE CODE 2")
            }
            records.append(record)
            
    with open(json_path, mode='w', encoding='utf-8') as f:
        json.dump(records, f, indent=2)
    print(f"Successfully converted {len(records)} records for crashdata.")

if __name__ == "__main__":
    convert_weather_data()
    convert_crash_data()
