import sqlite3

conn = sqlite3.connect("./weather.db")
cur = conn.cursor()

# Find the hottest hour today
for row in cur.execute("SELECT temperature_c FROM hourly_weather ORDER BY temperature_c DESC LIMIT 1;"):
    print("Hottest hour today:", row)

# Find the average temperature
for row in cur.execute("SELECT AVG(temperature_c) FROM hourly_weather;"):
    print("Average temp today:", row[0])

conn.close()
