## Librerias para el proceso de Webscraping ##
import re
import time
import io
import requests
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from colorama import Fore

## Librerias del proceso de ETL y reduccion ##
import os
import pandas as pd
import numpy as np
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from io import StringIO


## FUNCION PARA REDUCCION DE ARCHIVOS AL 5% ##

def reducir_archivo(dataframe):
    # Realizar las operaciones necesarias en el DataFrame
    dataframe_reducido = dataframe.sample(frac=0.05, random_state=123).reset_index(drop=True)

    return dataframe_reducido

## FIN DE FUNCION DE REDUCCION ##


## FUNCION DE PROCESADO DE ARCHIVOS A PARQUET ##

def procesar_datos_parquet(datos_parquet):
    # Leer el contenido Parquet desde bytes y convertirlo a DataFrame
    tabla_parquet = pq.read_table(io.BytesIO(datos_parquet))
    dataframe = tabla_parquet.to_pandas()

    # Resto de tu lógica de procesamiento de datos aquí...

    return dataframe

## FIN PROCESADO PARQUET ## 


## FUNCION PARA PROCESADO DE LOS ARCHIVOS YELLOW TAXIS QUE VAYAN INGRESANDO DEL WEBSCRAPING ##

def procesar_datos_yellow(datos):
    # Aquí puedes agregar tu lógica de ETL para transformar los datos según tus necesidades
    # Por ejemplo, podrías utilizar pandas para manipular datos tabulares si es necesario
    # datos = mi_logica_etl(datos)
    
    # Lectura de datos scrapeados
    #yellow_taxis = pd.read_parquet(datos)
    yellow_taxis = datos
    
    # Conversion de las columnas en tipo de dato str a fecha y hora
    yellow_taxis["tpep_pickup_datetime"] = pd.to_datetime(yellow_taxis["tpep_pickup_datetime"], format='%Y-%m-%d %H:%M:%S')
    yellow_taxis["tpep_dropoff_datetime"] = pd.to_datetime(yellow_taxis["tpep_dropoff_datetime"], format='%Y-%m-%d %H:%M:%S')

    # Creacion de Columna nueva
    yellow_taxis["trip_datatime"] = yellow_taxis["tpep_dropoff_datetime"] - yellow_taxis["tpep_pickup_datetime"]
    
    # Filtrados los campos donde el año sea menor a 2019 y eliminar esas filas del DataFrame
    yellow_taxis = yellow_taxis[yellow_taxis["tpep_pickup_datetime"].dt.year >= 2019]
    
    # Filtrados los campos donde el año sea menor a 2019 y eliminar esas filas del DataFrame
    yellow_taxis = yellow_taxis[yellow_taxis["tpep_dropoff_datetime"].dt.year >= 2019]
    
    # Elimincion de filas con valores nulos en la columna 'passenger_count'
    yellow_taxis = yellow_taxis.dropna(subset=['passenger_count'])
    
    # Conversion los 0s a NaNs en la columna 'passenger_count' utilizando .loc[]
    yellow_taxis.loc[yellow_taxis['passenger_count'] == 0, 'passenger_count'] = np.nan
    
    # Interpolar valores nulos en la columna 'passenger_count'
    yellow_taxis.loc[:, 'passenger_count'] = yellow_taxis['passenger_count'].interpolate(method='linear')

    # Convertir la columna 'passenger_count' a tipo entero
    yellow_taxis['passenger_count'] = yellow_taxis['passenger_count'].astype(int)
    
    # Dropeo de columnas de poca utilidad
    yellow_taxis.drop('store_and_fwd_flag', axis=1, inplace=True)
    yellow_taxis.drop('VendorID', axis=1, inplace=True)
    
    
    # Filtrado
    filtro_total_amount_mayores = yellow_taxis['total_amount'] > 10000
    
    # Eliminacion de las filas con valores mayor a 100000
    yellow_taxis = yellow_taxis.drop(yellow_taxis[filtro_total_amount_mayores].index)
    
    # Eliminacion de las filas que tienen total amount negativos
    yellow_taxis = yellow_taxis.drop(yellow_taxis[yellow_taxis['total_amount'] < 0].index)
    

    
    ## SUMA DE TAZAS Y CREACION DE COLUMNA 'TOTAL_FARE' ##
    
    # Se crea una lista con las columnas a sumar
    columnas_a_sumar = ['fare_amount', 'extra', 'mta_tax','tolls_amount','improvement_surcharge','congestion_surcharge']

    # Se crea una nueva columna 'total_fare' que contiene la suma de las columnas de costos
    yellow_taxis['total_fare'] = yellow_taxis[columnas_a_sumar].sum(axis=1)
    
    # Se crea una lista con las columnas a borrar
    columnasAborrar = ['fare_amount', 'extra', 'mta_tax','tolls_amount','improvement_surcharge','congestion_surcharge', 'total_amount']

    # Se dropean las columnas enlistadas
    yellow_taxis = yellow_taxis.drop(columnasAborrar, axis=1)
    
    ## FIN SUMA DE TAZAS Y CREACION DE COLUMNA 'TOTAL_FARE' ##
    
    
    
    # Extraccion del componente de tiempo en segundos y convertirlo a entero
    yellow_taxis['trip_datatime'] = pd.to_timedelta(yellow_taxis['trip_datatime']) 
    yellow_taxis['trip_time'] = yellow_taxis['trip_datatime'].dt.total_seconds().astype(int)
    
    
    # Renombrado de columnas
    yellow_taxis.rename(columns={'tip_amount': 'tips', 'trip_distance':'trip_miles'}, inplace=True)
    
    
    ## CONVERSION A DATETIME Y SEPARACION POR COLUMNAS ##
    
    # Lista de nombres de columnas que deseas convertir a tipo datetime
    columnas_a_convertir = ['tpep_pickup_datetime','tpep_dropoff_datetime']

    # Itera sobre cada columna y realiza la conversión a tipo datetime
    for columna in columnas_a_convertir:
        yellow_taxis[columna] = pd.to_datetime(yellow_taxis[columna], errors='coerce')
    
    # Crea dos nuevas columnas: 'fecha' y 'hora' para recogida
    yellow_taxis['PUDate'] = yellow_taxis['tpep_pickup_datetime'].dt.date
    yellow_taxis['PUTime'] = yellow_taxis['tpep_pickup_datetime'].dt.time

    # Crea dos nuevas columnas: 'fecha' y 'hora' para arribo
    yellow_taxis['DODate'] = yellow_taxis['tpep_dropoff_datetime'].dt.date
    yellow_taxis['DOTime'] = yellow_taxis['tpep_dropoff_datetime'].dt.time
    
    # Lista de columnas datetime a eliminar
    columnasAborrar = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

    # Eliminacion de columnas datetime
    yellow_taxis.drop(columnasAborrar, axis=1, inplace=True)
    
    #Ordenado de columnas
    orden = yellow_taxis[['PUDate','PUTime','PULocationID','PUBorough','PUZone','DODate','DOTime','DOLocationID','DOBorough','DOZone','trip_miles','trip_time','tips','passenger_count','payment_type','total_fare']]
    
    datos = orden
    
    return datos

## FIN DE FUNCION PARA PROCESADO DE ARCHIVOS YELLOW TAXI ##


## FUNCION PARA PROCESADO DE LOS ARCHIVOS DE UBER ##

def procesar_datos_uber (datos):
    
    # Lectura de datos scrapeados
    uber = datos.copy()
    
    # Filtrar el DataFrame por la condición 'hvfhs_license_num' == 'HV0003'
    uber = uber[uber['hvfhs_license_num'] == 'HV0003']
    
    # Se entiende que los registros nulos se deben a que son viajes que no fueron tomados en zona de aeropuertos, se procede a rellenarlos con valor tasa '0.00'
    uber.fillna(0.00, inplace=True)
    
    # Se crea una lista con las columnas a sumar
    columnas_a_sumar = ['base_passenger_fare', 'tolls', 'bcf','sales_tax','congestion_surcharge','airport_fee','driver_pay']

    # Se crea una nueva columna 'total_fare' que contiene la suma de las columnas de costos
    uber['total_fare'] = uber[columnas_a_sumar].sum(axis=1)
    
    # Se crea una lista con las columnas a borrar
    columnasAborrar = ['base_passenger_fare', 'tolls', 'bcf','sales_tax','congestion_surcharge','airport_fee','driver_pay']

    # Se dropean las columnas enlistadas
    uber_sumado = uber.drop(columnasAborrar, axis=1)
    
    # Se elimina la columna que contiene la hora en que llego el conductor al lugar de recogida.
    # Podria eliminarse tambien la columna de request_datetime, ya que a los objetivos del proyecto no estaría aportando algo valioso.
    columnasAborrar = ['on_scene_datetime', 'request_datetime']

    uber = uber_sumado.drop(columnasAborrar, axis=1)
    
    # Lista de nombres de columnas que deseas convertir a tipo datetime
    columnas_a_convertir = ['pickup_datetime','dropoff_datetime']

    # Itera sobre cada columna y realiza la conversión a tipo datetime
    for columna in columnas_a_convertir:
        uber[columna] = pd.to_datetime(uber[columna], errors='coerce')

    orden = ['pickup_datetime','PULocationID','PUBorough','PUZone','dropoff_datetime','DOLocationID','DOBorough','DOZone','trip_miles','trip_time','tips','total_fare']
    
    # Reordena las columnas según el nuevo orden
    uber = uber[orden]

    # Se renombran las columnas necesarias
    uber.rename(columns={'pickup_datetime': 'PUDatetime','dropoff_datetime' : 'DODatetime' }, inplace=True)

    # Crea dos nuevas columnas: 'fecha' y 'hora' -- para Pick Up y Drop Off
    uber['PUDate'] = uber['PUDatetime'].dt.date
    uber['PUTime'] = uber['PUDatetime'].dt.time

    uber['DODate'] = uber['DODatetime'].dt.date
    uber['DOTime'] = uber['DODatetime'].dt.time

    columnasAborrar = ['PUDatetime', 'DODatetime']

    uber = uber.drop(columnasAborrar, axis=1)

    orden = ['PUDate','PUTime','PULocationID','PUBorough','PUZone','DODate','DOTime','DOLocationID','DOBorough','DOZone','trip_miles','trip_time','tips','total_fare']
    
    # Reordena las columnas según el nuevo orden
    uber = uber[orden]
    
    return uber

## FIN UBER ##  



## FUNCION JOIN ## 

def realizar_join(datos_procesados, csv_url):
    try:
        # Descargar el contenido del archivo CSV
        response = requests.get(csv_url)
        response.raise_for_status()  # Lanzar excepción en caso de error de la solicitud

        # Convertir el contenido CSV a un DataFrame de pandas
        taxis_zones = pd.read_csv(StringIO(response.text))
        
        # Realizar ambos joins en una sola operación
        datos_procesados = pd.merge(
            datos_procesados, taxis_zones,
            left_on='PULocationID', right_on='LocationID', how='inner'
        ).rename(columns={'Borough': 'PUBorough', 'Zone': 'PUZone'}).drop('LocationID', axis=1)
        
        datos_procesados = pd.merge(
            datos_procesados, taxis_zones,
            left_on='DOLocationID', right_on='LocationID', how='inner'
        ).rename(columns={'Borough': 'DOBorough', 'Zone': 'DOZone'}).drop('LocationID', axis=1)
        
        # Convertir la columna 'PUBorough'  a cadena
        datos_procesados['PUBorough'] = datos_procesados['PUBorough'].astype(str)
        
        # Convertir la columna 'DOBorough'  a cadena
        datos_procesados['DOBorough'] = datos_procesados['DOBorough'].astype(str)
        

        # Convertir la columna 'PUZone'  a cadena
        datos_procesados['PUZone'] = datos_procesados['PUZone'].astype(str)

        # Convertir la columna 'DOZone'  a cadena
        datos_procesados['DOZone'] = datos_procesados['DOZone'].astype(str)

        print("Join realizado exitosamente.")
        return datos_procesados

    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud web o al procesar el CSV: {e}")
        return None
    except pd.errors.EmptyDataError:
        print("El archivo CSV está vacío.")
        return None
    except pd.errors.ParserError:
        print("Error al parsear el archivo CSV.")
        return None
    except Exception as e:
        print(f"Error inesperado: {e}")
        return None

## FIN FUNCION JOIN ##


## FUNCION MERGE Y STORAGE ##

def merge_and_upload(bucket_name, input_folder, output_folder, output_filename):
    # Inicializar el cliente de Cloud Storage
    client = storage.Client()

    # Obtener el bucket
    bucket = client.get_bucket(bucket_name)

    # Obtener la lista de objetos en la carpeta de entrada
    blobs = bucket.list_blobs(prefix=input_folder)

    # Lista para almacenar los DataFrames de cada archivo
    dataframes = []

    # Leer cada archivo y almacenar su DataFrame en la lista
    for blob in blobs:
        if blob.name.endswith('.parquet'):
            # Descargar el contenido del archivo
            blob_content = blob.download_as_bytes()

            # Convertir el contenido Parquet a DataFrame
            table = pq.read_table(io.BytesIO(blob_content))
            dataframe = table.to_pandas()

            # Agregar el DataFrame a la lista
            dataframes.append(dataframe)

    # Realizar el merge de todos los DataFrames
    merged_dataframe = pd.concat(dataframes, ignore_index=True)

    # Crear el objeto Parquet desde el DataFrame
    buffer = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pandas(merged_dataframe), buffer)
    parquet_bytes = buffer.getvalue().to_pybytes()

    # Subir el archivo fusionado a la carpeta de salida en Cloud Storage
    output_blob = bucket.blob(f"{output_folder}/{output_filename}")
    output_blob.upload_from_string(parquet_bytes)

    print(f"Merge completado y archivo guardado en {output_folder}/{output_filename}")



## FIN FUNCION MERGE ##


## SCRIPT WEBSCRAPING ##

website = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

resultado = requests.get(website, headers=headers)

try:
    resultado.raise_for_status()
    content = resultado.text
    
    csv_url =  "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"

    patrones = [r'<a\s+href="(.*?/trip-data/yellow_tripdata_(\d{4}-\d{2})\.parquet)"',
            r'<a\s+href="(.*?/trip-data/fhvhv_tripdata_(\d{4}-\d{2})\.parquet)"']
    
    for patron in patrones:
        maquinas = re.findall(patron, content)

        for url,fecha in maquinas:
            print(f"Procesando URL: {url}")
            
            # Extraer año y mes
            anio, mes = map(int, fecha.split('-'))
            
            if 2019 <= anio <= 2023 and fecha != '2023-12' and 'yellow_tripdata' in url:
                # Descargar el contenido del archivo
                response = requests.get(url, headers=headers)
                
                # Introducir una pausa de 1 segundo entre las solicitudes
                time.sleep(10)
                
                if response.status_code == 200:
                    print(f"Descargando y procesando archivo {url.split('/')[-1]}")
                    
                    # Procesar a parquet
                    datos_parquet = procesar_datos_parquet(response.content)
                    
                    # Reducir el archivo al 5%
                    datos_reducidos = reducir_archivo(datos_parquet)
                    
                    # Realizar un JOIN
                    datos_join = realizar_join(datos_reducidos, csv_url)
                    
                    # Procesar los datos descargados antes de cargarlos en Cloud Storage
                    datos_procesados = procesar_datos_yellow(datos_join)
                    
                    # Subir el contenido procesado a Cloud Storage
                    bucket_name = "proyectofinal_nyc"  # Reemplazar con el nombre de tu bucket
                    blob_path = f"yellow_taxis/{url.split('/')[-1]}"  # Ruta en Cloud Storage

                    client = storage.Client()
                    bucket = client.bucket(bucket_name)
                    blob = bucket.blob(blob_path)

                    # Convertir DataFrame Parquet a bytes
                    parquet_table = pa.Table.from_pandas(datos_procesados)
                    buffer = pa.BufferOutputStream()
                    pq.write_table(parquet_table, buffer)
                    parquet_bytes = buffer.getvalue().to_pybytes()

                    # Subir el contenido procesado a Cloud Storage
                    blob.upload_from_string(parquet_bytes)

                    print(f"Archivo {url.split('/')[-1]} subido a Cloud Storage en formato Parquet")
                    
                else:
                    print(f"Error al descargar URL {url}. Código de estado: {response.status_code}")
                
            
            else:
                # Añadir condición para detener el proceso cuando llegue a 2018
                if anio <= 2018:
                    print(f"URL {url} omitida (fuera del rango de años)")
                    continue
            
            
            if 2019 <= anio <= 2023 and fecha != '2023-12' and 'fhvhv_tripdata' in url:
                
                # Descargar el contenido del archivo
                response = requests.get(url, headers=headers)
                
                # Introducir una pausa de 1 segundo entre las solicitudes
                time.sleep(10)
                
                if response.status_code == 200:
                    print(f"Descargando y procesando archivo {url.split('/')[-1]}")
                    
                    # Procesar a parquet sale un dataframe
                    datos_parquet = procesar_datos_parquet(response.content)
                    
                    
                    # Reducir el archivo al 5%
                    datos_reducidos = reducir_archivo(datos_parquet)
                    
                    
                    # Realizar un JOIN
                    datos_join = realizar_join(datos_reducidos, csv_url)
                    
                    
                    # Procesar los datos descargados antes de cargarlos en Cloud Storage
                    datos_procesados = procesar_datos_uber(datos_join)
                    
                    
                    # Subir el contenido procesado a Cloud Storage
                    bucket_name = "proyectofinal_nyc"  # Reemplazar con el nombre de tu bucket
                    blob_path = f"uber/{url.split('/')[-1]}"  # Ruta en Cloud Storage
                    
                    client = storage.Client()
                    bucket = client.bucket(bucket_name)
                    blob = bucket.blob(blob_path)
                    
                    # Convertir DataFrame Parquet a bytes
                    parquet_table = pa.Table.from_pandas(datos_procesados)
                    buffer = pa.BufferOutputStream()
                    pq.write_table(parquet_table, buffer)
                    parquet_bytes = buffer.getvalue().to_pybytes()

                    blob.upload_from_string(parquet_bytes)

                    print(f"Archivo {url.split('/')[-1]} subido a Cloud Storage")
                
                else:
                    print(f"Error al descargar URL {url}. Código de estado: {response.status_code}")
            
            else:
                # Añadir condición para detener el proceso cuando llegue a 2018
                if anio <= 2018:
                    print(f"URL {url} omitida (fuera del rango de años)")
                    break
    print("Datos procesados y almacenados en Cloud Storage exitosamente.")
    
    ## LECTURA DE CLOUD STORAGE YELLOW TAXI Y CARGA EN YELLOW_TAXI_MERGE ##
    
    
    # Llamada a la función con los parámetros adecuados
    merge_and_upload("proyectofinal_nyc", "yellow_taxis", "yellow_taxis_merge", "yellow_taxi_merged.parquet")
    
    merge_and_upload("proyectofinal_nyc", "uber", "uber_merge", "uber_merged.parquet")

    
    ## FIN DE CARGA EN YELLOW_TAXI_MERGE ##

    print("Proceso completado exitosamente.")

except requests.exceptions.RequestException as e:
    print(f"Error en la solicitud web: {e}")
