# Databricks notebook source
# MAGIC %md
# MAGIC # SPARK MACHINE LEARNING

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM DATASET_DELITOS

# COMMAND ----------

# MAGIC %md
# MAGIC ## SPARK SESSION
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder \
    .appName("ml") \
    .getOrCreate()


# COMMAND ----------

# MAGIC %md
# MAGIC ## LOAD DATASET

# COMMAND ----------

table_name = "DATASET_DELITOS"  # Reemplaza con el nombre de tu tabla Hive
DATA = spark.table(table_name)


# COMMAND ----------

DATA.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ANALISIS EXPLORATORIO DE DE LOS DATOS

# COMMAND ----------

DATA.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### CLASES DE NUESTRO DATAFRAME

# COMMAND ----------

classes = df.select("TIPO_DELITO").distinct().collect()

# Imprimir las clases únicas
print("CLASES DE LA VARIABLE TIPO_DELITO:")
for row in classes:
    print(row[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## PREPROCESAMIENTO DE LA DATA  NLP

# COMMAND ----------

# MAGIC %md
# MAGIC ## IMPORTACION DE LAS LIBRERIAS

# COMMAND ----------

from pyspark.sql.functions import length
from pyspark.sql.functions import col, count, sum

# COMMAND ----------

DATA =DATA.select(col("DESCRIPCION"), col("TIPO_DELITO"))

# COMMAND ----------

DATA.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## NLP -  TECNICAS DE EXTRACCION DE CARACTERISTICAS

# COMMAND ----------

from pyspark.ml.feature import(Tokenizer,StopWordsRemover,CountVectorizer,IDF,StringIndexer)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  TOKENIZACIÓN

# COMMAND ----------


# dividims en palabras individuales  "DESCRIPTION"
tokenizer= Tokenizer(inputCol='DESCRIPCION', outputCol='token_description')

DATA=tokenizer.transform(DATA)

# COMMAND ----------

display(DATA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ELIMINACION DE PALABRAS VACIAS

# COMMAND ----------

# dado que no existe stopwords directamente en spañol , 
#usamos una bibliotca y obtenemos los stopswords en spñaol
%pip install  spacy
!pip install -U spacy
!python -m spacy download es_core_news_sm

# COMMAND ----------

import spacy

# CargaMOS el modelo de idioma español
nlp = spacy.load('es_core_news_sm')

# Obtenemos las palabras vacías en español
spanish_stopwords = nlp.Defaults.stop_words
spanish_stopwords_list = list(spanish_stopwords)
# Imprimimos la lista de palabras vacías en español
print(spanish_stopwords_list)

# COMMAND ----------

#eliminamos las palabras vacías (stop words) del texto. Se toma como entrada la columna "token_description" y el resultado se almacenará en una nueva columna llamada "stop_token". |This is an example sentence |[example, sentence] 
if 'stop_token' in DATA.columns:
    DATA = DATA.drop('stop_token')
stop_remove=StopWordsRemover(inputCol='token_description',outputCol='stop_token',stopWords=spanish_stopwords_list)

DATA = stop_remove.transform(DATA)

# COMMAND ----------

display(DATA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## APLICAMOS HASHING TF (Term of frecuency in corpus):
# MAGIC Esto asigna cada término a una posición única en un espacio vectorial y cuenta la frecuencia de cada término en el texto.

# COMMAND ----------

from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, Word2Vec, VectorAssembler

# COMMAND ----------

# Calculate term frequency CADA ARTICULO
hashing_tf = HashingTF(inputCol="stop_token",
                       outputCol="hashed_features", 
                       numFeatures=100)

# adds raw tf features to df
DATA= hashing_tf.transform(DATA)

# COMMAND ----------

display(DATA)
#indice : Representa los índices de las características no nulas en el vector.2 4 7 8 9 ...
#values: Corresponde a los valores asociados a cada uno de los índices en el vector. se proporciona una lista de valores que indica cuántas veces aparece cada característica en la noticia.

# COMMAND ----------

# MAGIC %md
# MAGIC ## IDF:
# MAGIC Se aplica el IDF para calcular la importancia relativa de cada término en el conjunto de documentos. Esto penaliza los términos que aparecen en muchos documentos y resalta los términos más informativos.

# COMMAND ----------

#calcular la importancia de cada término en función de su frecuencia en el corpus de texto
idf= IDF(inputCol='hashed_features',outputCol='tf_idf')

# COMMAND ----------

# MAGIC %md
# MAGIC ## WORD 2 VECT - LO QUE USAREMOS
# MAGIC
# MAGIC se utiliza para generar representaciones vectoriales densas de palabras, y no para secuencias de palabras ya procesadas, como las palabras después de la eliminación de palabras vacías.

# COMMAND ----------

word2vec = Word2Vec(vectorSize=100, minCount=5, inputCol='token_description', outputCol='word2vec_features')

# COMMAND ----------

model = word2vec .fit(DATA)


# COMMAND ----------

DATA=model.transform(DATA)

# COMMAND ----------

display(DATA)

# COMMAND ----------

from pyspark.sql.functions import size, expr


# COMMAND ----------

## para ver la longitud de los vectores generados por word2vect
row_index = 0  # Índice de la fila que deseas obtener

row_values = DATA.select('word2vec_features').collect()[row_index]['word2vec_features']



# COMMAND ----------

row_values.array.shape

# COMMAND ----------

row_values.array[1]

# COMMAND ----------

# MAGIC %md
# MAGIC ##  MODELADO - MACHINE LEARNING

# COMMAND ----------

# MAGIC %md
# MAGIC ### VECTOR ASSEMBLER : 

# COMMAND ----------


# 
# # Paso 3: Combina las características HashingTF y Word2Vec
#assembler = VectorAssembler(inputCols=['tf_idf', 'word2vec_features'], outputCol='features')
#reemplazado
    #assembler = VectorAssembler(inputCols=['tf_idf'], outputCol='features')
# para solo word2vect
assembler = VectorAssembler(inputCols=['word2vec_features'], outputCol='features')

# COMMAND ----------

# MAGIC %md
# MAGIC #  APLICAMOS STRINGINDEXER A MI COLUMNA DE SALIDA

# COMMAND ----------

from pyspark.ml.feature import StringIndexer


# COMMAND ----------

indexer = StringIndexer(inputCol='TIPO_DELITO', outputCol='label')


# COMMAND ----------

indexed= indexer.fit(DATA).transform(DATA)


# COMMAND ----------

indexed.show()

# COMMAND ----------

indexed.select('TIPO_DELITO', 'label').distinct().show()


# COMMAND ----------

labels = model.labels
print(labels)



# COMMAND ----------

# MAGIC %md
# MAGIC ### DIVISION DEL DATASET

# COMMAND ----------

from pyspark.ml.feature import StandardScaler

# COMMAND ----------

DATA_FINAL = DATA.select(col("DESCRIPCION"),col("TIPO_DELITO"))

# COMMAND ----------

DATA_FINAL.show()

# COMMAND ----------

(train_data, test_data) = DATA_FINAL.randomSplit([0.8, 0.2], seed=123)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCALAMOS EL TRAIN

# COMMAND ----------

Scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

# COMMAND ----------

# MAGIC %md
# MAGIC #  REGRESION LOGISTICA

# COMMAND ----------

from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier, DecisionTreeClassifier

from pyspark.ml import Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ###  DEFINICION DEL CLASIFICADOR 

# COMMAND ----------

logistic_regression = LogisticRegression(featuresCol='features', labelCol='label')

# COMMAND ----------

# MAGIC %md
# MAGIC ## CONSTRUCCION DEL PIPELINE

# COMMAND ----------

# contruccion del pipeline
#pipeline_lr = Pipeline(stages=[tokenizer, stop_remove, hashing_tf, idf, word2vec, assembler,indexer, logistic_regression])
#pipeline_lr = Pipeline(stages=[tokenizer, stop_remove, hashing_tf, idf,  assembler,indexer, logistic_regression])
#pipeline_lr = Pipeline(stages=[tokenizer, stop_remove, hashing_tf, word2vec,  assembler,indexer, logistic_regression])
# eliminando el stopremove
pipeline_lr = Pipeline(stages=[tokenizer, word2vec,  assembler,Scaler, indexer, logistic_regression])
#
model_lr = pipeline_lr.fit(train_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Predicciones con lo datos de prueba

# COMMAND ----------

predictions_lr = model_lr.transform(test_data)

# COMMAND ----------

predictions_lr.show()


# COMMAND ----------

row_index = 0  # Índice de la fila que deseas ver la predicción
prediction_row = predictions_lr.collect()[row_index]
prediction = prediction_row["prediction"]
print("Predicción:", prediction)


# COMMAND ----------

predictions_lr.select("DESCRIPCION", "prediction").show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluacion del modelo

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# COMMAND ----------

evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')

# COMMAND ----------

accuracy_lr = evaluator.evaluate(predictions_lr)

# COMMAND ----------

print('Logistic Regression Accuracy:', accuracy_lr)


# COMMAND ----------

evaluator.setMetricName("weightedPrecision")
precision = evaluator.evaluate(predictions_lr)
print("Precision:", precision)

# COMMAND ----------

# MAGIC %md
# MAGIC ## RECALL

# COMMAND ----------

evaluator.setMetricName("weightedRecall")
recall = evaluator.evaluate(predictions_lr)
print("Recall:", recall)
#aproximadamente el 63.38% de las instancias positivas en tu conjunto de datos fueron correctamente identificadas por el modelo.

# COMMAND ----------

evaluator.setMetricName("f1")
f1_score = evaluator.evaluate(predictions_lr)
print("F1-Score:", f1_score)

