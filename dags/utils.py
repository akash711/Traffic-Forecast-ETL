import psycopg2
from psycopg2 import Error
from configparser import ConfigParser
import requests
import json
import pandas as pd
import numpy as np
import holidays
import pickle
from airflow.models import Variable

CUR_DIR = os.path.abspath(os.path.dirname(__file__))


def config(filename='database.ini', section='postgresql'):

    """
    Parse config details from database.ini file
    """
    filename = f"{CUR_DIR}/database.ini"
    parser = ConfigParser()
    parser.read(filename)

    db = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def connect_to_db():

    """
    Connects to PostgreSQL database
    """

    db_config = config()


    try:
        # Connect to an existing database
        connection = psycopg2.connect(user = db_config['user'],
                                      password = db_config['password'],
                                      host = db_config['host'],
                                      database = db_config['database'])

        # Create a cursor to perform database operations
        cursor = connection.cursor()
        # Print PostgreSQL details
        print("PostgreSQL server information")
        print(connection.get_dsn_parameters(), "\n")
        # Executing a SQL query
        cursor.execute("SELECT version();")
        # Fetch result
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")
    
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

    return cursor, connection
    
        
def create_db():

    cursor, connection = connect_to_db()


    sql_query = """
        
        CREATE TABLE IF NOT EXISTS Weather
        (
            id SERIAL PRIMARY KEY NOT NULL,
            DateTime TIMESTAMPTZ,
            WindSpeed INTEGER,
            Temperature INTEGER,
            Precipitation BOOLEAN
        );
        """

    cursor.execute(sql_query)
    connection.commit()
    print('Database: Weather created!')
    sql_query = """
        
        CREATE TABLE IF NOT EXISTS Traffic
        (
            id SERIAL PRIMARY KEY NOT NULL,
            date_id BIGINT UNIQUE REFERENCES public.weather,
            Intensity INTEGER
        );
        """
    

    cursor.execute(sql_query)
    connection.commit()

    print('Database: Traffic created!')
    cursor.close()
    connection.close()


def convert_to_celc(fahrenheit):
    return int((fahrenheit - 32)* (5/9))


def extract_weather_data(location_url = 'http://dataservice.accuweather.com/locations/v1/cities/search/', query = 'Utrecht'):

    
    api_key = 'nPQT2nMFQG3ghVYazkU5jQ0P58DsVPNn'
    

    response = requests.get(location_url, params={'apikey':api_key, 'q': query, 'details': True})

    
    loc_codes = json.loads(response.content)
    for i in loc_codes:
        if i['EnglishName'] == query:
            location_key = i['Key']
        break

    forecast_api = f'http://dataservice.accuweather.com/forecasts/v1/hourly/12hour/{location_key}'
    response = requests.get(forecast_api, params={'apikey':api_key, "details": True})
    content = json.loads(response.content)

    weather_data = pd.DataFrame(columns = ['DateTime','WindSpeed','Temperature', 'Precipitation'])
    new_data = pd.DataFrame(columns = ['DateTime','WindSpeed','Temperature', 'Precipitation'], index = range(len(content)))
    
    for i in range(len(content)):
        
        if content[i]['Temperature']['Unit'] == 'F':
            temp = convert_to_celc(content[i]['Temperature']['Value'])
        
        else:
            temp = content[i]['Temperature']['Value']
        
        new_data.iloc[i,:] = pd.Series([pd.to_datetime(content[i]['DateTime']), int(content[i]['Wind']['Speed']['Value']), 
                                        temp, bool(content[i]['HasPrecipitation'])])

    weather_data = weather_data.append(new_data)

    return weather_data


def insert_into_weather_db():


    cursor, connection = connect_to_db()

    weather_data = extract_weather_data()

    for i in range(len(weather_data)):
        
        sql_query = """INSERT INTO Weather(DateTime, WindSpeed, Temperature, Precipitation) 
                                VALUES ('{}', {}, {}, {})
                                ON CONFLICT DO NOTHING;
                                """.format(weather_data.DateTime[i].strftime("%Y-%m-%d %H:%M:%S %z"), 
                                weather_data.WindSpeed[i], weather_data.Temperature[i], weather_data.Precipitation[i])
        
        cursor.execute(sql_query)
        connection.commit()

    cursor.close()
    connection.close()




def predict_traffic():

    weather_data = extract_weather_data()

    #Extract from DB

    nl_holidays = holidays.CountryHoliday('NL')

    model = pickle.load(open( f"{CUR_DIR}/RF_trafficmodel.p", "rb" ))

    df = pd.DataFrame(columns = ['Month', 'Day', 'Hour', 'DayofWeek', 'isHoliday', 'Wind Speed', 'Temperature', 'Precipitation'])

    df['Month'] = weather_data['DateTime'].apply(lambda x: x.month)
    df['Day'] = weather_data['DateTime'].apply(lambda x: x.day)
    df['Hour'] = weather_data['DateTime'].apply(lambda x: x.hour)
    df['DayofWeek'] = weather_data['DateTime'].apply(lambda x: x.weekday())
    df['isHoliday'] = weather_data['DateTime'].apply(lambda x: x in nl_holidays)
    df['Wind Speed'] = weather_data['WindSpeed']
    df['Temperature'] = weather_data['Temperature']
    df['Precipitation'] = weather_data['Precipitation'].astype('bool')

    traffic_counts = model.predict(np.array(df))

    df_traffic = pd.DataFrame(columns = ['DateTime', 'Intensity'], index = range(len(traffic_counts)))

    df_traffic['DateTime'] = weather_data['DateTime']
    df_traffic['Intensity'] = traffic_counts


    return df_traffic



def insert_into_traffic_db():

    df_traffic = predict_traffic()
    
    cursor, connection = connect_to_db()

    for i in range(len(df_traffic)):
        
        sql_query = """INSERT INTO Traffic(date_id, Intensity) 
                                VALUES ( (SELECT id FROM weather WHERE datetime = '{}') , {})
                                ON CONFLICT DO NOTHING;
                                """.format(df_traffic.DateTime[i].strftime("%Y-%m-%d %H:%M:%S %z"), 
                                df_traffic['Intensity'][i] * 10)
        
        cursor.execute(sql_query)
        connection.commit()

    cursor.close()
    connection.close()



create_db()
insert_into_weather_db()
insert_into_traffic_db()