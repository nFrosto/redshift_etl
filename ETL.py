import requests
import pandas as pd
from json import JSONDecodeError
import configparser
import psycopg2
import datetime
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


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

print(df_climatico_total)

# Consulta SQL para crear una tabla si no existe
create_table_query = """
CREATE TABLE IF NOT EXISTS weather_data (
    city VARCHAR(50),
    date DATE,
    temp_max DECIMAL(5,2),
    temp_min DECIMAL(5,2),
    feels_like_max DECIMAL(5,2),
    feels_like_min DECIMAL(5,2),
    humidity DECIMAL(5,2),
    precip_prob DECIMAL(5,2)
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
    df_climatico_total.to_sql('weather_data', connection,schema = "nicolas_ortizc_coderhouse", if_exists='append', index=False)
    connection.execute("commit")


