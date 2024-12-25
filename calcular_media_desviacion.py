from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, collect_list,count, mean

#Creación de sesión aunque ya exista al juntar el código
spark = SparkSession.builder.appName("Desviacion").getOrCreate()
rutacsv = "asistencia_limpio.csv"

#Carga de datos
df = spark.read.csv(rutacsv, header=True, inferSchema=True)
df.head(10)

#Número de alumnos por estado de asistencia
totales_por_estado = df.groupBy("estado_asistencia").agg(count("alumno").alias("total"))
totales_por_estado.show(5)

#Lista alumnos clasificados por asistencia
lista_por_asistencia = df.groupBy("estado_asistencia").agg(collect_list("alumno").alias("lista_alumnos"))
lista_por_asistencia.show(10)

#Calcular media y desviación por estado

#Totales por asignatura y estado
alumnos_asignatura_estado = df.groupBy("asignatura", "estado_asistencia").agg(count("alumno").alias("total"))
alumnos_asignatura_estado.show(50)

#Estadísticas
media_desviacion = alumnos_asignatura_estado.groupBy("asignatura").agg(mean("total").alias("media"),
                                                                      stddev("total").alias("desviación"))
media_desviacion.show()