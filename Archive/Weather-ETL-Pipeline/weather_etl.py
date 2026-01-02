import requests
import pandas as pd
from sqlalchemy import create_engine



# City Coordinates 
cities= {
    "Accra": {"lat": 5.55, "lon": -0.20},
"Kumasi": {"lat": 6.69, "lon": -1.63},
    "Takoradi": {"lat": 4.89, "lon": -1.75}
}
# APT endpoint
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 5.55,        # Accra latitude
    "longitude": -0.20,      # Accra longitude
    "hourly": "temperature_2m",
    "forecast_days": 1       # today only
}

# Extract 
response = requests.get(url, params=params)
data =  response.json()


# Transform
# Convert JSON to DataFrame
df = pd.DataFrame({
    "datetime": data["hourly"]["time"],
    "temperature_c": data["hourly"]["temperature_2m"]
})

# Convert datetime to pandas datetime
df["datetime"] = pd.to_datetime(df["datetime"])

print("Sample data:\n", df.head())


# Load 
engine = create_engine("sqlite:///weather.db")
df.to_sql("hourly_weather", engine, if_exists="replace", index=False)

print("ETL complete! Data saved into weather.db")

