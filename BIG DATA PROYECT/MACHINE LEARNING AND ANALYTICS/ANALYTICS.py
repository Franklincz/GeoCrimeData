# Databricks notebook source
# MAGIC %md
# MAGIC # CLASIFICACIÓN Y GEOLOCALIZACIÓN DE NOTICIAS DE INCIDENTES DELICTIVOS

# COMMAND ----------

# MAGIC %md
# MAGIC ### CARGA DE DATASET  ORIGINAL

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

raw_crimes = spark.read.csv("/mnt/crimesdl/raw/crimes_news.csv", sep=';', header=True)

# COMMAND ----------

display(raw_crimes)

# COMMAND ----------

## VISUALIZAR EL TOTAL DE REGISTROS
raw_crimes.count()

# COMMAND ----------

# escribimos una tabla estructurada en hive
raw_crimes.write.saveAsTable("Crimes")
## consulta SQL para eliminar la tabla
#spark.sql("DROP TABLE IF EXISTS Crimes")


# COMMAND ----------

## esto era para crear una vista temporal
#raw_crimes.createOrReplaceTempView("Crimes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### USAMOS SQL  PARA ANALYTICS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Crimes

# COMMAND ----------

# MAGIC %sql
# MAGIC --REALIZANDO UN DESCRIBE A LA DATA
# MAGIC DESCRIBE Crimes

# COMMAND ----------

# MAGIC %sql
# MAGIC --HACEMOS UN DISTINT A LA TABLA POR TIPO_DELITO
# MAGIC SELECT DISTINCT TIPO_DELITO
# MAGIC FROM Crimes;

# COMMAND ----------

# MAGIC %md
# MAGIC ### TIPOS DE DELITOS MÁS COMUNES 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT TIPO_DELITO, COUNT(*) AS CANTIDAD
# MAGIC FROM  Crimes
# MAGIC GROUP BY TIPO_DELITO
# MAGIC ORDER BY CANTIDAD DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gráfico de barras apiladas para mostrar la cantidad de delitos por tipo y distrito:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT TIPO_DELITO, DISTRITO, COUNT(*) AS CANTIDAD
# MAGIC FROM Crimes
# MAGIC GROUP BY TIPO_DELITO, DISTRITO
# MAGIC ORDER BY TIPO_DELITO, CANTIDAD DESC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUBSTRING(fecha_publicacion, 7, 4) AS anio, COUNT(*) AS cantidad_delitos
# MAGIC FROM Crimes
# MAGIC WHERE fecha_publicacion IS NOT NULL
# MAGIC GROUP BY SUBSTRING(fecha_publicacion, 7, 4)
# MAGIC ORDER BY anio ASC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geolocalización de los incidentes reportados de algunos

# COMMAND ----------

# MAGIC %md
# MAGIC ### 

# COMMAND ----------

#PAARA EL API
import json
import ast
import urllib.parse

# COMMAND ----------

API_KEY= 'AIzaSyA9BGUpFdPtvqyd4ZrgpwWCj94COZ8TM9Y'

# COMMAND ----------

## funcion par obtener latitud
import requests

def obtener_latitud_longitud(direccion, api_key):
    # Codifica la dirección para que sea válida en una URL
    direccion_codificada = requests.utils.quote(direccion)

    # URL de la API de Geocodificación de Google Maps
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={direccion_codificada}&key={api_key}"

    # Realiza la solicitud GET a la API
    response = requests.get(url)
    data = response.json()

    # Verifica si la solicitud fue exitosa y obtiene la latitud y longitud
    if response.status_code == 200 and data["status"] == "OK":
        resultado = data["results"][0]
        latitud = resultado["geometry"]["location"]["lat"]
        longitud = resultado["geometry"]["location"]["lng"]
        return latitud, longitud
    else:
        return None, None



# COMMAND ----------

address="xxx"
obtener_latitud_longitud(address,API_KEY)

# COMMAND ----------

# MAGIC %md
# MAGIC ##  DIBUJAR EL MAPA

# COMMAND ----------

import folium
from folium import plugins

# COMMAND ----------

df_delitos = spark.table("crimes")

# COMMAND ----------

df_delitos.columns

# COMMAND ----------

mapa = folium.Map(location=[0, 0], zoom_start=12)
folium.Marker(location=[0, 0], icon=folium.Icon(color='green')).add_to(mapa)
for row in df_delitos.collect():
    direccion = row["direccion"]
    distrito = row["Distrito"]

    if direccion is not None and distrito is not None:
        direccion_completa = f"{direccion}, {distrito},Perú"
        print(direccion_completa)
        latitud, longitud = obtener_latitud_longitud(direccion_completa, API_KEY)
        print(latitud, longitud)
        
        if latitud is not "none" and longitud is not "none":
            folium.Marker(location=[latitud, longitud], icon=folium.Icon(color='red')).add_to(mapa)


mapa


