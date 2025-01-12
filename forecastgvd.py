# MÉTODO 2 (por asignatura)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, date_format, avg
from statsmodels.tsa.arima.model import ARIMA
import pandas as pd
from datetime import timedelta

# Crear la sesión Spark
spark = SparkSession.builder.appName("PrediccionAsistenciaGlobal").getOrCreate()

# Leer el archivo CSV en PySpark
file_path = "asistencia_test.csv"
data = spark.read.csv(file_path, header=True, inferSchema=True)

# Formatear las fechas
data = data.withColumn("fecha", date_format(col("timestamp"), "yyyy-MM-dd"))

# Calcular totales diarios por asignatura y estado de asistencia
totales_diarios = data.groupBy(
    "asignatura", "estado_asistencia", "fecha"
).agg(count("alumno").alias("total_diario"))

# Convertir el DataFrame de PySpark a Pandas
totales_pandas = totales_diarios.toPandas()

# Ordenar y preparar las series temporales
totales_pandas["fecha"] = pd.to_datetime(totales_pandas["fecha"])
totales_pandas = totales_pandas.sort_values(by=["asignatura", "estado_asistencia", "fecha"])

# Almacenar resultados de predicciones
resultados_forecast = []

# Agrupar series temporales por asignatura y estado de asistencia
series_agrupadas = totales_pandas.groupby(["asignatura", "estado_asistencia"])

# Iterar sobre cada grupo (asignatura y estado de asistencia)
for (asignatura, estado), grupo in series_agrupadas:
    grupo = grupo.sort_values("fecha")
    grupo = grupo.set_index("fecha")
    grupo.index.freq = 'D'

    # Asegurarse de que hay suficientes datos
    if len(grupo) > 10:
        serie = grupo["total_diario"]
        ultimo_dia = grupo.index[-1]

        try:
            # Entrenamiento del modelo
            modelo = ARIMA(serie, order=(5, 1, 0)) # p, d, q
            ajuste = modelo.fit()

            # Se predice el día siguiente
            prediccion = ajuste.forecast(steps=1)

            # Calcular el día predicho
            dia_predicho = ultimo_dia + timedelta(days=1)

            # Almacenar el resultado
            resultados_forecast.append({
                "asignatura": asignatura,
                "dia": dia_predicho.strftime("%Y-%m-%d"),
                "estado_asistencia": estado,
                "numero_alumnos": round(prediccion.iloc[0])  # Redondear el valor
            })
        except Exception as e:
            print(f"Error al ajustar ARIMA para {asignatura} - {estado}: {e}")

# Convertimos los resultados a un DataFrame de Pandas
df_forecast = pd.DataFrame(resultados_forecast)

# Exportación de los resultados a un CSV
output_path = "forecast_por_asignatura1.csv"
df_forecast.to_csv(output_path, index=False)

print(f"Predicciones globales guardadas en: {output_path}")