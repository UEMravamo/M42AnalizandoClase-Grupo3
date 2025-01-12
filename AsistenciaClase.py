import pandas as pd
from pmdarima import auto_arima  
from pyspark.sql import SparkSession
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
        
    for estado in ["presente", "tarde", "ausente"]:
        grupo_estado = totales_pandas[totales_pandas["estado_asistencia"] == estado]

        if len(grupo_estado) > 10:
            try:
                # Serie temporal para ARIMA
                serie = grupo_estado.set_index("fecha")["total_diario"]
                modelo_auto = auto_arima(serie, seasonal=False, stepwise=True, trace=False)
                modelo = ARIMA(serie, order=modelo_auto.order)
                ajuste = modelo.fit()
                prediccion = ajuste.forecast(steps=1)
                # Actualiza los valores del forecast
                if estado == "presente":
                    forecast_resultados["presentes"] = round(prediccion.iloc[0])
                elif estado == "tarde":
                    forecast_resultados["tarde"] = round(prediccion.iloc[0])
                elif estado == "ausente":
                    forecast_resultados["ausentes"] = round(prediccion.iloc[0])
            except Exception as e:
                print(f"Error al ajustar ARIMA para {estado}: {str(e)}")

    return forecast_resultados