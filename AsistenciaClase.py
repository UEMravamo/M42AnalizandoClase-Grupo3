from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime

spark = SparkSession.builder.appName("PreparacionDatosAsistencia").getOrCreate()

# Función de mostrar la asistencia ademas de limpieza de datos NULL
def mostrar_asistencia(csv_asistencia, csv_nulos, csv_filtrado):

    df = spark.read.csv(csv_asistencia, header=True, inferSchema=False) # Leer el archivo CSV
     
    df_nulos = df.filter(" OR ".join([f"{col} IS NULL" for col in df.columns])) # Filtrar los valores NULL
    df_nulos = df_nulos.orderBy("timestamp", "asignatura", "estado_asistencia") # Ordenacion de los valores por timestamp, asignatura, estado_asistencia
    df_nulos.write.csv(csv_nulos, header=True, mode="overwrite") # Guardar las filas con valores NULL en un archivo CSV aparte

    df_filtrado = df.dropna() # Eliminar los valores nulos y guardarlos en un nuevo dataframe
    df_filtrado = df_filtrado.orderBy("timestamp", "asignatura", "estado_asistencia") # Ordenacion de los valores por timestamp, asignatura, estado_asistencia
    df_filtrado.write.csv(csv_filtrado, header=True, mode="overwrite") # Guardar el dataframe filtrado en un CSV aparte
    
    df_filtrado.show() # Mostrar la estructura del DataFrame
    return df_filtrado # Retornar el DataFrame

ruta_asistencia = "././asistencia2500.csv" # Ruta del archivo CSV
ruta_nulos = "././nulos.csv"  # Ruta de los nulos
ruta_filtrado = "././filtrado.csv" # Ruta filtrado

registro_asistencia = mostrar_asistencia(ruta_asistencia, ruta_nulos, ruta_filtrado) # Llamada a la funcion