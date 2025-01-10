#MÉTODO 1 (perspectiva alumno)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, date_format, avg
from statsmodels.tsa.arima.model import ARIMA
import pandas as pd
from datetime import timedelta

# Crear la sesión Spark
spark = SparkSession.builder.appName("PrediccionAsistencia").getOrCreate()

# Leer el archivo CSV en PySpark
file_path = "asistencia_test.csv"
data = spark.read.csv(file_path, header=True, inferSchema=True)

# Pasar estado a valor numérico
data = data.withColumn(
    "estado_codificado",
    when(col("estado_asistencia") == "presente", 1)
    .when(col("estado_asistencia") == "tarde", 0.5)
    .otherwise(0)
)

# Agrupamos por alumno, asignatura y día y calcular estado promedio
series_temporales = data.groupBy(
    "alumno",
    "asignatura",
    date_format("timestamp", "yyyy-MM-dd").alias("fecha") # Pasar timestamp a fecha
).agg(
    avg("estado_codificado").alias("estado_promedio")  # Calcula media del estado
)

# Convertir el dataframe de PySpark a Pandas
series_pandas = series_temporales.toPandas()

# Ordenar los datos
series_pandas['fecha'] = pd.to_datetime(series_pandas['fecha'])
series_pandas = series_pandas.sort_values(by=['alumno', 'asignatura', 'fecha'])

# Almacenar resultados de predicciones
resultados_predicciones = []

# ARIMA para cada alumno y asignatura
for (alumno, asignatura), grupo in series_pandas.groupby(["alumno", "asignatura"]):
    grupo = grupo.sort_values("fecha")

    # Ordenar la lista por fecha
    grupo = grupo.set_index("fecha")
    grupo.index.freq = 'D'  # Frecuencia diaria

    serie = grupo["estado_promedio"]

    # Solo entrenamos si hay suficientes datos
    if len(serie) > 10:
        try:
            # Entrenamiento del modelo
            modelo = ARIMA(serie, order=(1, 1, 0)) # p, d, q
            ajuste = modelo.fit()

            # Se predice el día siguiente
            predicciones = ajuste.forecast(steps=1)
            ultima_fecha = grupo.index[-1]

            for i, pred in enumerate(predicciones):
                 dia_predicho = ultima_fecha + timedelta(days=i+1)
                 estado_predicho = pred
                 estado_categoria = (
                     "presente" if estado_predicho >= 0.75 else
                     "tarde" if estado_predicho >= 0.25 else
                     "ausente"
                 )

            resultados_predicciones.append({
                    "alumno": alumno,
                    "asignatura": asignatura,
                    "dia_predicho": dia_predicho.strftime("%Y-%m-%d"),
                    "estado_predicho_valor": estado_predicho,
                    "estado_predicho_categoria": estado_categoria
                })
        except Exception as e:
            print(f"Error al ajustar ARIMA para {alumno}, {asignatura}: {e}")

# Convertimos los resultados a un df de Pandas
df_resultados = pd.DataFrame(resultados_predicciones)

# Exportación de los resultados a un CSV
output_path = "forecast_por_alumno.csv"
df_resultados.to_csv(output_path, index=False)

print(f"Predicciones guardadas en: {output_path}")