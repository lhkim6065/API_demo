from fastapi import FastAPI
from sqlalchemy import create_engine, text
import yaml
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL is None:
    raise ValueError("The DATABASE_URL environment variable MUST be set!")
if not DATABASE_URL.startswith("postgresql"):
   DATABASE_URL = f"postgresql://{DATABASE_URL}"

app = FastAPI()
eng = create_engine(DATABASE_URL)

def create_simple_endpoint(endpoint, query):
   """Function to manufacture simple endpoints for those without much
   Python experience.
   """
   @app.get(endpoint)
   def auto_simple_endpoint():
      f"""Automatic endpoint function for {endpoint}"""
      with eng.connect() as con:
         res = con.execute(query)
         return [r._asdict() for r in res]
            
with open("endpoints.yaml") as f:
   endpoints = yaml.safe_load(f)
   for endpoint, query in endpoints.items():
      create_simple_endpoint(endpoint, query)





#------------------------------------------------
# Custom Endpoints
#------------------------------------------------

@app.get("/avgsptemp_byhour_day/{page}")
def movies_by_page(page, content=None):
     with eng.connect() as con:
        query = """
        SELECT CASE EXTRACT(DOW FROM binned_datetime_bin)
           WHEN 0 THEN 'Sunday'
           WHEN 1 THEN 'Monday'
           WHEN 2 THEN 'Tuesday'
           WHEN 3 THEN 'Wednesday'
           WHEN 4 THEN 'Thursday'
           WHEN 5 THEN 'Friday'
           WHEN 6 THEN 'Saturday'
           ELSE 'Unknown'
       END AS day_of_week
    FROM weather_traffic
    GROUP BY EXTRACT(DOW FROM binned_datetime_bin)
    ORDER BY EXTRACT(DOW FROM binned_datetime_bin)
    OFFSET :off
                """
        if content is not None:
            query = """
            SELECT *
            FROM weather_traffic
            WHERE binned_datetime_bin ~ :match
            OFFSET :off
            """
        res = con.execute(text(query), {'off': 50*int(page), 'match':content})
        return [r._asdict() for r in res]

