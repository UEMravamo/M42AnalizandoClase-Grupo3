import pandas as pd
from pmdarima import auto_arima 
from datetime import timedelta
from statsmodels.tsa.arima.model import ARIMA
from pyspark.sql.functions import col, date_format, max, count, collect_list, mean, stddev

def generar_forecast_por_asignatura(data):
    data = data.withColumn("fecha", date_format(col("timestamp"), "yyyy-MM-dd"))

    # Calcular totales diarios por estado
    totales_diarios = data.groupBy("estado_asistencia", "fecha").agg(count("alumno").alias("total_diario"))
    totales_pandas = totales_diarios.toPandas()
    totales_pandas["fecha"] = pd.to_datetime(totales_pandas["fecha"])
    totales_pandas = totales_pandas.sort_values(by=["estado_asistencia", "fecha"])

    # Diccionario inicializado para almacenar resultados del forecast
    forecast_resultados = {"presentes": 0, "tarde": 0, "ausentes": 0}
