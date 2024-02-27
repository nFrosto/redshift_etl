import requests
import pandas as pd
from json import JSONDecodeError
import configparser
import psycopg2
import datetime
from sqlalchemy import create_engine


config = configparser.ConfigParser()
config.read('config.ini')

cities = ["Santiago", "Madrid", "Cordoba", "Berlin"]

# Fecha de inicio (hoy) y fecha de fin (hoy + 7 días)
start_date = datetime.date.today()
end_date = start_date + datetime.timedelta(days=10)
formatted_start_date = start_date.strftime('%Y-%m-%d')
formatted_end_date = end_date.strftime('%Y-%m-%d')

# Lista para almacenar los datos climáticos de todas las ciudades
datos_climaticos_lista = []

for city in cities:
    url = f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{formatted_start_date}/{formatted_end_date}'
    params = {
        'key': config['weather_api']['key'],
        'unitGroup': 'metric',
        'include': 'days',
        'elements': 'datetime,tempmax,tempmin,feelslikemax,feelslikemin,humidity,precipprob' 
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Levanta una excepción para respuestas de error

        data = response.json()
        if 'days' in data and len(data['days']) > 0:
            for day_data in data['days']:  
                datos_climaticos = {
                    'city': city,
                    'date': day_data.get('datetime', 'N/A'),
                    'temp_max': day_data.get('tempmax', 'N/A'),
                    'temp_min': day_data.get('tempmin', 'N/A'),
                    'feels_like_max': day_data.get('feelslikemax', 'N/A'),
                    'feels_like_min': day_data.get('feelslikemin', 'N/A'),
                    'humidity': day_data.get('humidity', 'N/A'),
                    'precip_prob': day_data.get('precipprob', 'N/A')
                }

                datos_climaticos_lista.append(datos_climaticos)

        else:
            print(f"La respuesta para {city} no contiene datos climáticos.")

    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud para {city}: {e}")
    except JSONDecodeError:
        print(f"Error al decodificar la respuesta JSON para {city}.")

# Crear DataFrame a partir de la lista de datos
df_climatico_total = pd.DataFrame(datos_climaticos_lista)
df_climatico_total['actual_date'] = pd.Timestamp.now()

print(df_climatico_total)

# Consulta SQL para crear una tabla si no existe
create_table_query = """
CREATE TABLE IF NOT EXISTS stg_weather_data (
    city VARCHAR(50),
    date DATE,
    temp_max DECIMAL(5,2),
    temp_min DECIMAL(5,2),
    feels_like_max DECIMAL(5,2),
    feels_like_min DECIMAL(5,2),
    humidity DECIMAL(5,2),
    precip_prob DECIMAL(5,2),
    actual_date DATE,
    CONSTRAINT weather_pk PRIMARY KEY (city, date)
);

CREATE TABLE IF NOT EXISTS weather_data (
    city VARCHAR(50),
    date DATE,
    temp_max DECIMAL(5,2),
    temp_min DECIMAL(5,2),
    feels_like_max DECIMAL(5,2),
    feels_like_min DECIMAL(5,2),
    humidity DECIMAL(5,2),
    precip_prob DECIMAL(5,2),
    actual_date DATE,
    CONSTRAINT weather_pk PRIMARY KEY (city, date)
);
"""

# Establecer la conexión con Redshift
try:
    conn = psycopg2.connect(
        dbname= config['redshift']['database'], 
        user=config['redshift']['user'], 
        password=config['redshift']['password'], 
        host=config['redshift']['host'], 
        port=config['redshift']['port']
    )
    cur = conn.cursor()

    cur.execute(create_table_query)
    
    conn.commit()  

    print("Tabla creada con éxito o ya existía.")
except psycopg2.Error as e:
    print(f"Error en la conexión o al ejecutar la consulta SQL: {e}")
finally:
    if cur: cur.close()
    if conn: conn.close()

connection_string = f"postgresql://{config['redshift']['user']}:{config['redshift']['password']}@{config['redshift']['host']}:{config['redshift']['port']}/{config['redshift']['database']}"

engine = create_engine(connection_string)

with engine.connect() as connection:
    conn.execute('TRUNCATE TABLE stg_weather_data')
    df_climatico_total.to_sql(
        'stg_weather_data'
        ,connection
        ,schema = "nicolas_ortizc_coderhouse"
        ,if_exists='append'
        ,index=False #evita agregar el indice por defecto del df de pandas
        ,method = 'multi'#insertamos filas en lote
        )
    conn.execute("""
    MERGE into weather_data
    using stg_weather_data AS stg
    ON weather_data.city = stg.city
    AND weather_data.date = stg.date
    WHEN MATCHED THEN 
    UPDATE 
    SET weather_data.temp_max = stg.temp_max
    SET weather_data.temp_min = stg.temp_min
    SET weather_data.feels_like_max = stg.feels_like_max
    SET weather_data.feels_like_min = stg.feels_like_min               
    SET weather_data.humidity = stg.humidity       
    SET weather_data.precip_prob = stg.precip_prob    
    SET weather_data.actual_date = stg.actual_date            
    when NOT MATCHED THEN
    INSERT(city, date, temp_max, temp_min, feels_like_max, feels_like_min, humidity, precip_prob, actual_date)        
    """)
