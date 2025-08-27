import requests
import pandas as pd
from sqlalchemy import create_engine


# City Coordinates
cities = {
    "Accra": {"lat": 5.55, "lon": -0.20},
    "Kumasi": {"lat": 6.69, "lon": -1.63},
    "Takoradi": {"lat": 4.89, "lon": -1.75}
}


# Extract + Transform
all_data = []

for city, coords in cities.items():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "hourly": "temperature_2m",
        "forecast_days": 1
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    # Build DataFrame
    df = pd.DataFrame({
        "datetime": data["hourly"]["time"],
        "temperature_c": data["hourly"]["temperature_2m"]
    })
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["city"] = city  # add city column
    
    all_data.append(df)

# Combine all cities into one DataFrame
final_df = pd.concat(all_data, ignore_index=True)

print("Sample data:\n", final_df.head())

# Load
engine = create_engine("sqlite:///weather.db")
final_df.to_sql("hourly_weather", engine, if_exists="replace", index=False)

print("✅ ETL complete! Data saved into weather.db")
