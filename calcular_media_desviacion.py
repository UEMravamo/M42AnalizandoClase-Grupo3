from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, collect_list, count, mean

# Creación de sesión, aunque ya exista al juntar el código
spark = SparkSession.builder.appName("Desviacion").getOrCreate()

# Ruta del archivo CSV
ruta_csv = "asistencia_limpio.csv"

# Carga de datos
df = spark.read.csv(ruta_csv, header=True, inferSchema=True)
df.head(10)

# Número de alumnos por estado de asistencia
totales_por_estado = (
    df.groupBy("estado_asistencia")
    .agg(count("alumno").alias("total"))
)
totales_por_estado.show(5)

# Lista de alumnos clasificados por estado de asistencia
lista_por_asistencia = (
    df.groupBy("estado_asistencia")
    .agg(collect_list("alumno").alias("lista_alumnos"))
)
lista_por_asistencia.show(10)

# Totales por asignatura y estado de asistencia
alumnos_asignatura_estado = (
    df.groupBy("asignatura", "estado_asistencia")
    .agg(count("alumno").alias("total"))
)
alumnos_asignatura_estado.show(50)

# Estadísticas: Media y desviación por asignatura
media_desviacion = (
    alumnos_asignatura_estado.groupBy("asignatura")
    .agg(
        mean("total").alias("media"),
        stddev("total").alias("desviación")
    )
)
media_desviacion.show()
