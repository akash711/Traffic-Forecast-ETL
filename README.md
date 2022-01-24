# Traffic-Forecast-ETL

ETL pipeline for a simple traffic forecasting application using Airflow. <br>

Data sources:
- Historical data (Weather data: KNMI, Traffic data: Data Overheid NL)
- Real time Data (AccuWeather API) <br>

<img src="TrafficPriject.png" width="48">

ETL:
- Extract data from weather API
- Transform into appropriate format to give as input to trained ML model.
- Load to PostgreSQL database 

![ETL](/TrafficETL.png)

Database Schema:
- Traffic table (Fact table)
  - id (PK)
  - date_id (FK) references weather
  - Intensity (1-10) level of traffic
- Weather table
  - id (PK)
  - datetime 
  - windspeed
  - temperature
  - precipitation
 <br>
 Airflow DAG: <br>
 create_db >> insert_into_weather >> insert_into_traffic
