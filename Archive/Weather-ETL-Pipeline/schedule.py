import time 
import schedule
import weather_etl2

def job(): 
    print("Running daily ETL...")
    weather_etl2.main() # place the ETL code inside the 'main()' function 
    print("Done")

# Schedule once per day at 08:00
schedule.every().day.at("8:00").do(job)

print("Scheduler started. Waiting for jobs....")

while True: 
    schedule.run_pending()
    time.sleep(60)