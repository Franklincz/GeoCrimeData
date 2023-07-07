# Databricks notebook source
# MAGIC %md
# MAGIC ## INSTALACION DE LIBRERIAS

# COMMAND ----------

# MAGIC %pip install beautifulsoup4
# MAGIC %pip install nltk
# MAGIC %pip install  spacy
# MAGIC !pip install -U spacy
# MAGIC !python -m spacy download es_core_news_sm

# COMMAND ----------

# MAGIC %md
# MAGIC ## IMPORTANDO LIBRERIAS

# COMMAND ----------

import urllib.request # realizar solicitudes HTTP, como enviar solicitudes GET y POST a servidores web
import urllib.parse # Para parsear  y manipular URL. Puedes utilizarla para dividir una URL en sus componentes (protocolo, host, ruta, parámetros, etc.)
import json
import requests
from bs4 import BeautifulSoup  # extraer información de páginas web en HTML o XML
import time  # manipular el tiempo 
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC ## SPARK SESSION

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType  # para trabajar con los esquemas de datos de DataFrames.

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder \
    .appName("MiAplicacion") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC # INICIANDO OBTENCIÓN DE LA DATA

# COMMAND ----------

# MAGIC %md
# MAGIC ## FORMA 1

# COMMAND ----------

# Crear un RDD vacío
ContentNoticiasRDD = spark.sparkContext.emptyRDD()
feedOffset = 0
start_time = time.time()
while time.time() - start_time < 100:
    current_time = time.time()
    response = urllib.request.urlopen("https://elcomercio.pe/pf/api/v3/content/fetch/story-feed-by-section?query=%7B%22feedOffset%22%3A" + str(feedOffset)+"%2C%22includedFields%22%3A%22%26_sourceInclude%3Dwebsites.elcomercio.website_url%2C_id%2Cheadlines.basic%2Csubheadlines.basic%2Cdisplay_date%2Ccontent_restrictions.content_code%2Ccredits.by._id%2Ccredits.by.name%2Ccredits.by.url%2Ccredits.by.type%2Ccredits.by.image.url%2Cwebsites.elcomercio.website_section.path%2Cwebsites.elcomercio.website_section.name%2Ctaxonomy.sections.path%2Ctaxonomy.sections._id%2Ctaxonomy.sections.name%2Cpromo_items.basic.type%2Cpromo_items.basic.url%2Cpromo_items.basic.width%2Cpromo_items.basic.height%2Cpromo_items.basic.resized_urls%2Cpromo_items.basic_video.promo_items.basic.url%2Cpromo_items.basic_video.promo_items.basic.type%2Cpromo_items.basic_video.promo_items.basic.resized_urls%2Cpromo_items.basic_gallery.promo_items.basic.url%2Cpromo_items.basic_gallery.promo_items.basic.type%2Cpromo_items.basic_gallery.promo_items.basic.resized_urls%2Cpromo_items.youtube_id.content%2Cpromo_items.basic_html%2Cpromo_items.basic_jwplayer.type%2Cpromo_items.basic_jwplayer.subtype%2Cpromo_items.basic_jwplayer.embed%2Cpromo_items.basic_jwplayer.embed.config%2Cpromo_items.basic_jwplayer.embed.config.thumbnail_url%2Cpromo_items.basic_jwplayer.embed.config.resized_urls%2Cpromo_items.basic_jwplayer.embed.config.key%2Cpromo_items.basic_html.content%22%2C%22presets%22%3A%22landscape_s%3A234x161%2Clandscape_xs%3A118x72%22%2C%22section%22%3A%22%2Flima%22%2C%22stories_qty%22%3A100%7D&filter=%7Bcontent_elements%7B_id%2Ccontent_restrictions%7Bcontent_code%7D%2Ccredits%7Bby%7Bimage%7Burl%7D%2Cname%2Ctype%2Curl%7D%7D%2Cdisplay_date%2Cheadlines%7Bbasic%7D%2Cpromo_items%7Bbasic%7Bresized_urls%7Blandscape_s%2Clandscape_xs%2Clazy_default%7D%2Ctype%2Curl%7D%2Cbasic_gallery%7Bpromo_items%7Bbasic%7Bresized_urls%7Blandscape_s%2Clandscape_xs%2Clazy_default%7D%2Ctype%2Curl%7D%7D%7D%2Cbasic_html%7Bcontent%7D%2Cbasic_jwplayer%7Bembed%7Bconfig%7Bresized_urls%7Blandscape_s%2Clandscape_xs%2Clazy_default%7D%2Cthumbnail_url%7D%7D%2Csubtype%2Ctype%7D%2Cbasic_video%7Bpromo_items%7Bbasic%7Bresized_urls%7Blandscape_s%2Clandscape_xs%2Clazy_default%7D%2Ctype%2Curl%7D%7D%7D%2Cyoutube_id%7Bcontent%7D%7D%2Csubheadlines%7Bbasic%7D%2Ctaxonomy%7Bsections%7Bname%2Cpath%7D%7D%2Cwebsite_url%2Cwebsites%7Belcomercio%7Bwebsite_section%7Bname%2Cpath%7D%2Cwebsite_url%7D%7D%7D%2Cnext%7D&d=2764&_website=elcomercio")
    contenido = response.read()

    # Parse JSON data
    data = json.loads(contenido)
    ContentNoticias = data['content_elements']
    # Agregar los content_elements al RDD
    ContentNoticiasRDD = ContentNoticiasRDD.union(sc.parallelize(ContentNoticias))
    feedOffset += 100
    print("Tiempo actual: ", round(current_time - start_time, 2), "Noticias obtenidas: ", len(ContentNoticias))
    #time.sleep(1)  # Espera 1 segundo antes de hacer la siguiente solicitud

# COMMAND ----------

# MAGIC %md
# MAGIC ## FORMA 2

# COMMAND ----------

# Crear un RDD vacío
ContentNoticiasRDD2 = spark.sparkContext.emptyRDD()
feedOffset = 0
start_time = time.time()
while time.time() - start_time < 5:
    current_time = time.time()
    response = urllib.request.urlopen("https://elcomercio.pe/pf/api/v3/content/fetch/story-feed-by-section?query=%7B%22feedOffset%22%3A" + str(feedOffset)+"%2C%22includedFields%22%3A%22%26_sourceInclude%3Dwebsites.elcomercio.website_url%2C_id%2Cheadlines.basic%2Csubheadlines.basic%2Cdisplay_date%2Ccontent_restrictions.content_code%2Ccredits.by._id%2Ccredits.by.name%2Ccredits.by.url%2Ccredits.by.type%2Ccredits.by.image.url%2Cwebsites.elcomercio.website_section.path%2Cwebsites.elcomercio.website_section.name%2Ctaxonomy.sections.path%2Ctaxonomy.sections._id%2Ctaxonomy.sections.name%2Cpromo_items.basic.type%2Cpromo_items.basic.url%2Cpromo_items.basic.width%2Cpromo_items.basic.height%2Cpromo_items.basic.resized_urls%2Cpromo_items.basic_video.promo_items.basic.url%2Cpromo_items.basic_video.promo_items.basic.type%2Cpromo_items.basic_video.promo_items.basic.resized_urls%2Cpromo_items.basic_gallery.promo_items.basic.url%2Cpromo_items.basic_gallery.promo_items.basic.type%2Cpromo_items.basic_gallery.promo_items.basic.resized_urls%2Cpromo_items.youtube_id.content%2Cpromo_items.basic_html%2Cpromo_items.basic_jwplayer.type%2Cpromo_items.basic_jwplayer.subtype%2Cpromo_items.basic_jwplayer.embed%2Cpromo_items.basic_jwplayer.embed.config%2Cpromo_items.basic_jwplayer.embed.config.thumbnail_url%2Cpromo_items.basic_jwplayer.embed.config.resized_urls%2Cpromo_items.basic_jwplayer.embed.config.key%2Cpromo_items.basic_html.content%22%2C%22presets%22%3A%22landscape_s%3A234x161%2Clandscape_xs%3A118x72%22%2C%22section%22%3A%22%2Flima%22%2C%22stories_qty%22%3A100%7D&filter=%7Bcontent_elements%7B_id%2Ccontent_restrictions%7Bcontent_code%7D%2Ccredits%7Bby%7Bimage%7Burl%7D%2Cname%2Ctype%2Curl%7D%7D%2Cdisplay_date%2Cheadlines%7Bbasic%7D%2Cpromo_items%7Bbasic%7Bresized_urls%7Blandscape_s%2Clandscape_xs%2Clazy_default%7D%2Ctype%2Curl%7D%2Cbasic_gallery%7Bpromo_items%7Bbasic%7Bresized_urls%7Blandscape_s%2Clandscape_xs%2Clazy_default%7D%2Ctype%2Curl%7D%7D%7D%2Cbasic_html%7Bcontent%7D%2Cbasic_jwplayer%7Bembed%7Bconfig%7Bresized_urls%7Blandscape_s%2Clandscape_xs%2Clazy_default%7D%2Cthumbnail_url%7D%7D%2Csubtype%2Ctype%7D%2Cbasic_video%7Bpromo_items%7Bbasic%7Bresized_urls%7Blandscape_s%2Clandscape_xs%2Clazy_default%7D%2Ctype%2Curl%7D%7D%7D%2Cyoutube_id%7Bcontent%7D%7D%2Csubheadlines%7Bbasic%7D%2Ctaxonomy%7Bsections%7Bname%2Cpath%7D%7D%2Cwebsite_url%2Cwebsites%7Belcomercio%7Bwebsite_section%7Bname%2Cpath%7D%2Cwebsite_url%7D%7D%7D%2Cnext%7D&d=2764&_website=elcomercio")
    contenido = response.read()

    # Parse JSON data
    data = json.loads(contenido)
    ContentNoticias = data['content_elements']
    # Convertir el contenido en una lista de tuplas utilizando map y lambda
    rows = spark.sparkContext.parallelize(ContentNoticias).map(lambda content: (json.dumps(content),))

    # Agregar las tuplas al RDD
    ContentNoticiasRDD2 = ContentNoticiasRDD2.union(rows)

    feedOffset += 100
    print("Tiempo actual: ", round(current_time - start_time, 2), "Noticias obtenidas: ", len(ContentNoticias))
    #time.sleep(1)  # Espera 1 segundo antes de hacer la siguiente solicitud

# COMMAND ----------

## FORMA 3

# COMMAND ----------


# Crear un RDD vacío
ContentNoticiasRDD3 = spark.sparkContext.emptyRDD()

feedOffset = 0
start_time = time.time()
while time.time() - start_time < 5:
    current_time = time.time()
    response = urllib.request.urlopen("https://elcomercio.pe/pf/api/v3/content/fetch/story-feed-by-section?query=%7B%22feedOffset%22%3A" + str(feedOffset) + "%2C%22iwebsite=elcomercio")
    contenido = response.read()

    # Parsear los datos JSON
    data = json.loads(contenido)
    ContentNoticias = data['content_elements']

    # Convertir el contenido en una lista de tuplas utilizando map y lambda
    rows = spark.sparkContext.parallelize(ContentNoticias).map(lambda content: (json.dumps(content),))

    # Agregar las tuplas al RDD
    ContentNoticiasRDD2 = ContentNoticiasRDD2.union(rows)

    feedOffset += 100
    print("Tiempo actual: ", round(current_time - start_time, 2), "Noticias obtenidas: ", len(ContentNoticias))

# Crear el DataFrame a partir del RDD
df = spark.createDataFrame(ContentNoticiasRDD2, ["content"])

# Mostrar el DataFrame
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## FORMA 4 - USANDO DATAFRAMES - + OPTIMA

# COMMAND ----------

https://elcomercio.pe/pf/api/v3/content/fetch/story-feed-by-section?query=%7B%22feedOffset%22%3A100%2C%22includedFields%22%3A%22%26_sourceInclude%3Dwebsites.elcomercio.website_url%2C_id%2Cheadlines.basic%2Csubheadlines.basic%2Cdisplay_date%2Ccontent_restrictions.content_code%2Ccredits.by._id%2Ccredits.by.name%2Ccredits.by.url%2Ccredits.by.type%2Ccredits.by.image.url%2Cwebsites.elcomercio.website_section.path%2Cwebsites.elcomercio.website_section.name%2Ctaxonomy.sections.path%2Ctaxonomy.sections._id%2Ctaxonomy.sections.name%2Cpromo_items.basic.type%2Cpromo_items.basic.url%2Cpromo_items.basic.width%2Cpromo_items.basic.height%2Cpromo_items.basic.resized_urls%2Cpromo_items.basic_video.promo_items.basic.url%2Cpromo_items.basic_video.promo_items.basic.type%2Cpromo_items.basic_video.promo_items.basic.resized_urls%2Cpromo_items.basic_gallery.promo_items.basic.url%2Cpromo_items.basic_gallery.promo_items.basic.type%2Cpromo_items.basic_gallery.promo_items.basic.resized_urls%2Cpromo_items.youtube_id.content%2Cpromo_items.basic_html%2Cpromo_items.basic_jwplayer.type%2Cpromo_items.basic_jwplayer.subtype%2Cpromo_items.basic_jwplayer.embed%2Cpromo_items.basic_jwplayer.embed.config%2Cpromo_items.basic_jwplayer.embed.config.thumbnail_url%2Cpromo_items.basic_jwplayer.embed.config.resized_urls%2Cpromo_items.basic_jwplayer.embed.config.key%2Cpromo_items.basic_html.content%22%2C%22presets%22%3A%22landscape_s%3A234x161%2Clandscape_xs%3A118x72%22%2C%22section%22%3A%22%2Flima%22%2C%22stories_qty%22%3A100%7D&filter=%7Bcontent_elements%7B_id%2Ccontent_restrictions%7Bcontent_code%7D%2Ccredits%7Bby%7Bimage%7Burl%7D%2Cname%2Ctype%2Curl%7D%7D%2Cdisplay_date%2Cheadlines%7Bbasic%7D%2Cpromo_items%7Bbasic%7Bresized_urls%7Blandscape_s%2Clandscape_xs%2Clazy_default%7D%2Ctype%2Curl%7D%2Cbasic_gallery%7Bpromo_items%7Bbasic%7Bresized_urls%7Blandscape_s%2Clandscape_xs%2Clazy_default%7D%2Ctype%2Curl%7D%7D%7D%2Cbasic_html%7Bcontent%7D%2Cbasic_jwplayer%7Bembed%7Bconfig%7Bresized_urls%7Blandscape_s%2Clandscape_xs%2Clazy_default%7D%2Cthumbnail_url%7D%7D%2Csubtype%2Ctype%7D%2Cbasic_video%7Bpromo_items%7Bbasic%7Bresized_urls%7Blandscape_s%2Clandscape_xs%2Clazy_default%7D%2Ctype%2Curl%7D%7D%7D%2Cyoutube_id%7Bcontent%7D%7D%2Csubheadlines%7Bbasic%7D%2Ctaxonomy%7Bsections%7Bname%2Cpath%7D%7D%2Cwebsite_url%2Cwebsites%7Belcomercio%7Bwebsite_section%7Bname%2Cpath%7D%2Cwebsite_url%7D%7D%7D%2Cnext%7D&d=2782&_website=elcomercio

# COMMAND ----------


df4 = spark.createDataFrame([], schema="content STRING")
feed_offset = 0
start_time = time.time()
while time.time() - start_time < 100:
    current_time = time.time()
    response = urllib.request.urlopen("https://elcomercio.pe/pf/api/v3/content/fetch/story-feed-by-section?query=%7B%22feedOffset%22%3A" + str(feed_offset) + "%2C%22includedFields%22%3A%22%26_sourceInclude%3Dwebsites.elcomercio.website_url%2C_id%2Cheadlines.basic%2Csubheadlines.basic%2Cdisplay_date%2Ccontent_restrictions.content_code%2Ccredits.by._id%2Ccredits.by.name%2Ccredits.by.url%2Ccredits.by.type%2Ccredits.by.image.url%2Cwebsites.elcomercio.website_section.path%2Cwebsites.elcomercio.website_section.name%2Ctaxonomy.sections.path%2Ctaxonomy.sections._id%2Ctaxonomy.sections.name%2Cpromo_items.basic.type%2Cpromo_items.basic.url%2Cpromo_items.basic.width%2Cpromo_items.basic.height%2Cpromo_items.basic.resized_urls%2Cpromo_items.basic_video.promo_items.basic.url%2Cpromo_items.basic_video.promo_items.basic.type%2Cpromo_items.basic_video.promo_items.basic.resized_urls%2Cpromo_items.basic_gallery.promo_items.basic.url%2Cpromo_items.basic_gallery.promo_items.basic.type%2Cpromo_items.basic_gallery.promo_items.basic.resized_urls%2Cpromo_items.youtube_id.content%2Cpromo_items.basic_html%2Cpromo_items.basic_jwplayer.type%2Cpromo_items.basic_jwplayer.subtype%2Cpromo_items.basic_jwplayer.embed%2Cpromo_items.basic_jwplayer.embed.config%2Cpromo_items.basic_jwplayer.embed.config.thumbnail_url%2Cpromo_items.basic_jwplayer.embed.config.resized_urls%2Cpromo_items.basic_jwplayer.embed.config.key%2Cpromo_items.basic_html.content%22%2C%22presets%22%3A%22landscape_s%3A234x161%2Clandscape_xs%3A118x72%22%2C%22section%22%3A%22%2Flima%22%2C%22stories_qty%22%3A100%7D&filter=%7Bcontent_elements%7B_id%2Ccontent_restrictions%7Bcontent_code%7D%2Ccredits%7Bby%7Bimage%7Burl%7D%2Cname%2Ctype%2Curl%7D%7D%2Cdisplay_date%2Cheadlines%7Bbasic%7D%2Cpromo_items%7Bbasic%7Bresized_urls%7Blandscape_s%2Clandscape_xs%2Clazy_default%7D%2Ctype%2Curl%7D%2Cbasic_gallery%7Bpromo_items%7Bbasic%7Bresized_urls%7Blandscape_s%2Clandscape_xs%2Clazy_default%7D%2Ctype%2Curl%7D%7D%7D%2Cbasic_html%7Bcontent%7D%2Cbasic_jwplayer%7Bembed%7Bconfig%7Bresized_urls%7Blandscape_s%2Clandscape_xs%2Clazy_default%7D%2Cthumbnail_url%7D%7D%2Csubtype%2Ctype%7D%2Cbasic_video%7Bpromo_items%7Bbasic%7Bresized_urls%7Blandscape_s%2Clandscape_xs%2Clazy_default%7D%2Ctype%2Curl%7D%7D%7D%2Cyoutube_id%7Bcontent%7D%7D%2Csubheadlines%7Bbasic%7D%2Ctaxonomy%7Bsections%7Bname%2Cpath%7D%7D%2Cwebsite_url%2Cwebsites%7Belcomercio%7Bwebsite_section%7Bname%2Cpath%7D%2Cwebsite_url%7D%7D%7D%2Cnext%7D&d=2788&_website=elcomercio")
    contenido = response.read()

    # Parsear los datos JSON
    data = json.loads(contenido)
    content_noticias = data['content_elements']

    # Convertir los content_elements en tuplas utilizando map y lambda
    content_tuplas = map(lambda content: (json.dumps(content),), content_noticias)

    # Convertir el resultado de map en un DataFrame
    content_df = spark.createDataFrame(content_tuplas, schema="content STRING")

    # Unir el DataFrame existente con el nuevo DataFrame
    df4 = df4.union(content_df)

    feed_offset += 100
    print("Tiempo actual: ", round(current_time - start_time, 2), "Noticias obtenidas: ", len(data['content_elements']))

# Mostrar el DataFrame resultante
df4.show(2)


# COMMAND ----------

df4.printSchema()

# COMMAND ----------

# Supongamos que queremos imprimir la primera fila del DataFrame
first_row = df4.collect()[0]

# Acceder a los valores de la fila utilizando la notación de puntos
print(first_row.content)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MOSTRAR CANTIDAD DE REGISTROS OBTENIDOS PARA CADA CASO

# COMMAND ----------

# MAGIC %md
# MAGIC #### CANTIDAD DE NOTICIAS OBTENIDAS PARA CASO 4

# COMMAND ----------

# PARA LA FORMA 4 
total_count1 = df4.count()
print("Total de content_elements forma 1:", total_count1)
# PARA LA FRMA 2



# COMMAND ----------

## DEFINIMOS EL ESQUEMA DE NUESTRO DATAFRAME A USAR PARA ALMACENAR LAS NOTICIAS

# COMMAND ----------

# Definimos el esquema del DataFrame
schema = StructType([
    StructField("TITULO", StringType(), True),
    StructField("DESCRIPCION", StringType(), True),
    StructField("FECHA_PUBLICACION", StringType(), True)
])

# Creamos el DataFrame vacío
noticias_df = spark.createDataFrame([], schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CUANDO SE USA LA FORMA 1

# COMMAND ----------

def get_noticia(noticia):
    url_noticia = noticia['websites']['elcomercio']['website_url']
    urlfinal = 'https://elcomercio.pe/' + url_noticia
    response = requests.get(urlfinal)
    html_content = response.content
    soup = BeautifulSoup(html_content, 'html.parser')
    titulo_element = (
        soup.find("div", class_="f just-center")
        and soup.find("div", class_="f just-center").find("h1", itemprop="name", class_="sht__title")
    )
    titulo = titulo_element.get_text() if titulo_element else "none"
    descripcion_element = (
        soup.find("div", class_="story-contents w-full")
        and soup.find("div", class_="story-contents w-full").find_all("p", itemprop="description")
    )
    descripcion_texto = ""
    if descripcion_element:
        for p in descripcion_element:
            descripcion_texto += p.get_text()
    else:
        descripcion_texto = "none"
    return (titulo, descripcion_texto)
    

# COMMAND ----------

# Aplicar la función get_noticia_info a cada elemento del RDD ContentNoticias
resultados = ContentNoticiasRDD.map(get_noticia)
resultados.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## PARA LA FORMA 2

# COMMAND ----------

def get_noticia_info2(noticia):
    content_json = json.loads(noticia[0])  # Analizar la cadena JSON dentro de la tupla
    url_noticia = content_json['websites']['elcomercio']['website_url']
    urlfinal = 'https://elcomercio.pe/' + url_noticia
    response = requests.get(urlfinal)
    html_content = response.content
    soup = BeautifulSoup(html_content, 'html.parser')
    titulo_element = (
        soup.find("div", class_="f just-center")
        and soup.find("div", class_="f just-center").find("h1", itemprop="name", class_="sht__title")
    )
    titulo = titulo_element.get_text() if titulo_element else "none"
    descripcion_element = (
        soup.find("div", class_="story-contents w-full")
        and soup.find("div", class_="story-contents w-full").find_all("p", itemprop="description")
    )
    descripcion_texto = ""
    if descripcion_element:
        for p in descripcion_element:
            descripcion_texto += p.get_text()
    else:
        descripcion_texto = "none"
    return (titulo, descripcion_texto)

# COMMAND ----------

noticias_info_rdd2 = ContentNoticiasRDD2.map(get_noticia_info2)

# COMMAND ----------

noticias_info_rdd2.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## CUANDO SE USA LA FORMA 4

# COMMAND ----------

def get_noticia4(noticia):
    noticia_json = json.loads(noticia)
    url_noticia =  noticia_json ['websites']['elcomercio']['website_url']
    urlfinal = 'https://elcomercio.pe/' + url_noticia
    response = requests.get(urlfinal)
    html_content = response.content.decode('utf-8')
    soup = BeautifulSoup(html_content, 'html.parser')
    titulo_element = (
        soup.find("div", class_="f just-center")
        and soup.find("div", class_="f just-center").find("h1", itemprop="name", class_="sht__title")
    )
    titulo = titulo_element.get_text() if titulo_element else "none"
    descripcion_element = (
        soup.find("div", class_="story-contents w-full")
        and soup.find("div", class_="story-contents w-full").find_all("p", itemprop="description")
    )
    descripcion_texto = ""
    if descripcion_element:
        for p in descripcion_element:
            descripcion_texto += p.get_text()
    else:
        descripcion_texto = "none"

    #  PARA OBTENER LA FECHA PUBLICACION 
    fecha_element = soup.find("div", class_="f just-center")
    if fecha_element:
        fecha_element = fecha_element.find_all("div", class_="s-aut__time")
        if fecha_element:
            fecha_element = fecha_element[0].find("time", datetime=True)
            fecha_publicacion = fecha_element["datetime"] if fecha_element else None
        else:
            fecha_publicacion = None
    else:
        fecha_publicacion = None

    return titulo, descripcion_texto,fecha_publicacion


# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd




# Convertir la función en un UDF
get_noticia_udf = udf(get_noticia4, schema)

# Aplicar la función al DataFrame
result_df44 = df4.withColumn('result', get_noticia_udf(df4['content']))

# Obtener columnas individuales a partir de la columna 'result'
result_df44 = result_df44.select('content', 'result.titulo', 'result.descripcion','result.fecha_publicacion')


# COMMAND ----------

result_df44.printSchema()

# COMMAND ----------

result_df44= result_df44.select("titulo","descripcion","fecha_publicacion")

# COMMAND ----------

# Mostrar el DataFrame actualizado
result_df44.show(5)

# COMMAND ----------

#result_df44
display(result_df44)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PARA GUARDAR EN CSV

# COMMAND ----------

# X EJEMPLO TENGO un DataFrame llamado "ruta_guardado" y deseas guardarlo como un archivo CSV
ruta_guardado = "/FileStore/BIGDATA/OutputScrapping.csv"
result_df44 = result_df44.coalesce(1)
# Guardar el DataFrame como archivo CSV
result_df44.write.csv(ruta_guardado, header=True, mode="overwrite")

# COMMAND ----------

ruta_guardado = "/FileStore/BIGDATA/OutputFINALDATASETScrapping"
result_df44 = result_df44.coalesce(1)
result_df44.write.option("header", "true").option("charset", "UTF-8").mode("overwrite").csv(ruta_guardado)


# COMMAND ----------

# MAGIC %md
# MAGIC ## PARA LLEVAR EL ARCHIVO A MI LOCAL 

# COMMAND ----------

dbfs:/FileStore/BIGDATA/OutputFINALDATASETScrapping
dbfs:/FileStore/BIGDATA/OutputFINALDATASETScrapping/_committed_1973699137010283581.csv

# COMMAND ----------

https://community.cloud.databricks.com/?o=5251589843994025

# COMMAND ----------

dbfs:/FileStore/BIGDATA/OutputFINALDATASETScrapping/_committed_1973699137010283581

# COMMAND ----------

https://community.cloud.databricks.com/files/BIGDATA/OutputFINALDATASETScrapping/part-00000-tid-1973699137010283581-fe073e93-4fc6-43c8-86af-346a55ea019d-101-1-c000.csv?o=5251589843994025

# COMMAND ----------

# MAGIC %md
# MAGIC ## POR AHORA USAREMOS LA FORMA 4 . USANDO DATAFRAME

# COMMAND ----------

# MAGIC %md
# MAGIC #### FILTRAMOS SOLO LAS NOTICIAS DE DELITOS O QUE TENGAN QUE VER CON DELITOS A TRAVES DE PALABRAS CLAVE

# COMMAND ----------

import nltk
from nltk.corpus import stopwords
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType

# COMMAND ----------


# Definir la lista de palabras y sus lemas personalizados
custom_lemmas = {
    "violado": "violar",
    "asesinado": "asesinar",
    "matado": "matar",
    "homicidio": "homicidio",
    "delincuencia": "delincuencia",
    "robo": "robar",
    "roban":"robar",
    "secuestran": "secuestrar",
    "fraude": "fraudar",
    "estafan": "estafar",
    "estafado":"estafar",
    "estafas":"estafar",
    "cupos":"cupo",
    "clonan":"clonar",
    "clonación":"clonar",
    "clonaron":"clonar",
    "corrupción": "corromper",
    "drogas": "drogar",
    "violencia": "violentar",
    "violación":"violar",
    "violada":"violar",
    "violacion":"violar",
    "crimen": "cometer",
    "impunidad": "quedar impune",
    "agresión": "agredir",
    "amenaza": "amenazar",
    "extorsión": "extorsionar",
    "falsificación": "falsificar",
    "cárcel": "encarcelar",
    "prisión": "encarcelar",
    "víctima": "victimar",
    "testigo": "testificar",
    "investigación": "investigar",
    "interrogatorio": "interrogar",
    "procesar": "procesar",
    "juez": "juzgar",
    "abuso": "abusar",
    "abusan":"abusar",
    "matan": "matar",
    "capturan":"capturar",
    "capturado":"capturar",
    "robo a mano armada": "robar a mano armada",
    "violación sexual": "violar sexualmente",
    "atentado": "atentar",
    "corrupción policial": "corromper policialmente",
    "tráfico de drogas": "traficar drogas",
    "banda criminal": "banda criminal",
    "terrorismo": "aterrorizar",
    "secuestro express": "secuestrar rápidamente",
    "estafa financiera": "estafar financieramente",
    "crimen organizado": "organizar crímenes",
    "fraude bancario": "defraudar bancos",
    "extorsión telefónica": "extorsionar por teléfono",
    "robo de identidad": "robar identidades",
    "fraudes":"fraude",
    "agredió":"agreder",
    "agrede":"agreder",
    "violó":"violar",
    "asesinó":"asesinar",
    "apuñaló":"apuñalar",
    "asesinato en serie": "asesinar en serie",
    "corrupción gubernamental": "corromper gubernamentalmente",
    "homicidio culposo": "homicidio accidental",
    "fraude electoral": "defraudar electoralmente",
    "agresión física": "agredir físicamente",
    "terrorismo internacional": "aterrorizar internacionalmente",
    "secuestro de menores": "secuestrar menores",
    "tráfico de armas": "traficar armas",
    "pandilla criminal": "pandilla criminal",
    "asesinato por encargo": "asesinar por encargo",
    "fraude de seguros": "defraudar seguros",
    "extorsión cibernética": "extorsionar en línea",
    "robo a domicilio": "robar en domicilios",
    "violación de derechos humanos": "violar derechos humanos",
    "corrupción empresarial": "corromper empresarialmente",
    "homicidio doloso": "homicidio intencional",
    "fraude fiscal": "defraudar fiscal"}

# Cargar el modelo de idioma español en Spacy
nlp = spacy.load('es_core_news_sm')

# Función para lematizar una palabra
def lemmatize_word(word):
    if word in custom_lemmas:
        return custom_lemmas[word]
    doc = nlp(word)
    return doc[0].lemma_

# Definimos una función UDF para verificar si un título contiene palabras relacionadas a delitos
contains_delito_udf = udf(lambda titulo: contains_delito(titulo), returnType=StringType())

# Definims una función para verificar si un título contiene palabras relacionadas a delitos
def contains_delito(titulo):
    titulo_palabras = titulo.split()
    titulo_lemmatized = [lemmatize_word(word) for word in titulo_palabras]
    return any(word in custom_lemmas.values() for word in titulo_lemmatized)



# COMMAND ----------

# Aplicar la función de verificación y filtrar las noticias relacionadas a delitos
noticias_delito_df = result_df44.filter(contains_delito_udf(result_df44. titulo) == lit(True))

# Mostrar el nuevo DataFrame con las noticias relacionadas a delitos
noticias_delito_df.show()

# COMMAND ----------

noticias_delito_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## LEYENDO NOTICIAS EXTRAIDAS EN UN DATAFRAME Y HACIENDO RECONOCIMIENTO DE ENTIDADES

# COMMAND ----------

from pyspark.sql.functions import concat, concat_ws, col, lit
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

# COMMAND ----------


# Convertimos uan la lista de distritos en un conjunto para una búsqueda más eficiente
lima_districts = ["Ancón", "Ate", "Barranco", "Breña", "Carabayllo", "Chaclacayo", "Chorrillos", "Cieneguilla",
                  "Comas", "El Agustino", "Independencia", "Jesús María", "La Molina", "La Victoria", "Lince",
                  "Los Olivos", "Lurigancho", "Lurín", "Magdalena del Mar", "Miraflores", "Pachacámac", "Pucusana",
                  "Pueblo Libre", "Puente Piedra", "Punta Hermosa", "Punta Negra", "Rímac", "San Bartolo",
                  "San Borja", "San Isidro", "San Juan de Lurigancho","Chosica", "San Juan de Miraflores", "San Luis",
                  "San Martín de Porres", "San Miguel", "Santa Anita", "Santa María del Mar", "Santa Rosa",
                  "Santiago de Surco", "Surquillo", "Villa El Salvador", "Villa María del Triunfo","Surco"]
lima_districts_set = set(lima_districts) #aqui un  conjunto (set) ,q es una colección desordenada de elementos únicos, lo que significa que no puede contener duplicados.

# Definimos la función de búsqueda de entidades y distritos
def find_entities_and_districts(text):
    doc = nlp(text)
    #entities = set([ent.text for ent in doc.ents if ent.label_ == "LOC"])
    districts = set([district for district in lima_districts_set if district.lower() in text.lower()])
    #return list(entities.union(districts))
    return list(districts)

# Registra la función UDF en Spark
extract_entities_udf = udf(find_entities_and_districts, ArrayType(StringType()))


# COMMAND ----------

result_df = noticias_delito_df .withColumn("ubicacion", extract_entities_udf("descripcion"))

# COMMAND ----------

result_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import concat_ws  #permite concatenar los valores de una o varias columnas en una cadena utilizando un delimitador especificado.
# transformar el DataFrame result_df al concatenar los valores de la columna "ubicacion" en una cadena separada por comas y luego renombran la columna resultante como "ubicacion".
result_df = result_df.withColumn("ubicacion_str", concat_ws(",", result_df["ubicacion"]))
result_df=result_df.drop("ubicacion").withColumnRenamed("ubicacion_str", "ubicacion")

# COMMAND ----------

result_df.show(5)

# COMMAND ----------

# Obtener los primeros 1000 registros del DataFrame original
dataframe1 = result_df.head(1000)

# COMMAND ----------

display(dataframe1)

# COMMAND ----------

dataframe2 = result_df.limit(2000).collect()[1000:]

# COMMAND ----------

display(dataframe2)

# COMMAND ----------

ruta_guardado = "/FileStore/BIGDATA/DATASET_DELITOS"
result_df = result_df.coalesce(10)
result_df.write.option("header", "true").option("charset", "UTF-8").mode("overwrite").csv(ruta_guardado)

# COMMAND ----------

df_koalas = result_df.to_koalas() # se puede ajustar el umbral para equilibrar la precisión y el rendimiento

# COMMAND ----------

df_koalas.to_csv('/FileStore/BIGDATA/DATASET_DELITOS/koalasDataframe.csv', index=False)

# COMMAND ----------

display(result_df)
