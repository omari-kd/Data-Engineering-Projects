import sqlite3

conn = sqlite3.connect("weather.db")
cur = conn.cursor()

# Get hottest hour per city
print("Hottest hour per city:")
for row in cur.execute("""
    SELECT city, datetime, MAX(temperature_c)
    FROM hourly_weather
    GROUP BY city;
"""):
    print(row)

# Compare average temps
print("Average temperature per city:")
for row in cur.execute("""
    SELECT city, AVG(temperature_c)
    FROM hourly_weather
    GROUP BY city;
"""):
    print(row)

conn.close()
