
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, max
from pyspark.sql.window import Window


# In[226]:


spark = SparkSession.builder \
    .appName("Forecast de Asistencia") \
    .getOrCreate()


# In[228]:


file_path = "asistencia.csv"


# In[230]:


data = spark.read.csv(file_path, header=True, inferSchema=True)

data.show(5)
data.printSchema()


# In[232]:


data = data.withColumn("fecha", to_date(col("timestamp")))
data.show(5)


# In[234]:


ventana_tiempo = Window.partitionBy("asignatura", "estado_asistencia") \
                       .orderBy("fecha") \
                       .rowsBetween(-6, 0)


# In[236]:


data_forecast = data.withColumn("media_movil", avg("estado_asistencia").over(ventana_tiempo))


# In[238]:


data_forecast.select("asignatura", "estado_asistencia", "fecha", "media_movil").show()


# In[240]:


ultima_fecha = data.groupBy("asignatura", "estado_asistencia") \
                   .agg(max("fecha").alias("ultima_fecha"))


# In[242]:


ultima_fecha = ultima_fecha.withColumnRenamed("asignatura", "asignatura_ultima") \
                           .withColumnRenamed("estado_asistencia", "estado_asistencia_ultima")

forecast_diario = data_forecast.join(
    ultima_fecha,
    (data_forecast["asignatura"] == ultima_fecha["asignatura_ultima"]) &
    (data_forecast["estado_asistencia"] == ultima_fecha["estado_asistencia_ultima"]) &
    (data_forecast["fecha"] == ultima_fecha["ultima_fecha"])
).select(
    data_forecast["asignatura"],
    data_forecast["estado_asistencia"],
    data_forecast["media_movil"]
)


forecast_diario.show()


# In[248]:


forecast_csv_path = "forecast_diario_csv"
forecast_json_path = "forecast_diario_json"

