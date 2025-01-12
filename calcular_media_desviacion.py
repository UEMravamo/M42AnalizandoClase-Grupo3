import json
import warnings
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, max, count, collect_list, mean, stddev
from statsmodels.tsa.arima.model import ARIMA
from pmdarima import auto_arima  # Auto_arima para que se haga una predicción decente sin tener que hacer un ajuste manual de los parámetros
import pandas as pd
from datetime import timedelta

# Función para calcular métricas diarias, media y desviación por estados y asignaturas y posterior guardado en JSON junto con el forecast
def calcular_metricas_y_forecast_json(df, json_output):
    # Obtener asignaturas una única vez
    asignaturas_df = df.select("asignatura").distinct()
    # Lista para almacenar resultados
    data_json = []

    # Procesar cada asignatura
    for asignatura_row in asignaturas_df.collect():
        # ¿Por qué collect? Porque para este caso en el que los datos que traemos desde PySpark no son un número que comprometa la memoria y rendimiento del PC, es buen caso para utilizarlo
        asignatura = asignatura_row["asignatura"]
        
        # Datos por asignatura
        df_asignatura = df.filter(col("asignatura") == asignatura)

        # Obtener el último día de registros para calcular datos del día actual (suponemos que es el día más reciente)
        ultimo_dia = df_asignatura.agg(
            max(date_format(col("timestamp"), "yyyy-MM-dd")).alias("ultimo_dia")
        ).first()["ultimo_dia"]

        # Filtrar registros del último día
        df_ultimo_dia = df_asignatura.filter(date_format(col("timestamp"), "yyyy-MM-dd") == ultimo_dia)

        # Calcular totales del último día por agrupacion 
        totales_estado = df_ultimo_dia.groupBy("estado_asistencia").agg(
            count("alumno").alias("total"),
            collect_list("alumno").alias("alumnos")
        ).collect()

        # Crear diccionario para totales del día actual
        totales_dia_actual = {
            "presentes": next((row["total"] for row in totales_estado if row["estado_asistencia"] == "presente"), 0),
            "tarde": next((row["total"] for row in totales_estado if row["estado_asistencia"] == "tarde"), 0),
            "ausentes": next((row["total"] for row in totales_estado if row["estado_asistencia"] == "ausente"), 0),
            }

        alumnos_por_estado = {
            "presentes": next((row["alumnos"] for row in totales_estado if row["estado_asistencia"] == "presente"), []),
            "tarde": next((row["alumnos"] for row in totales_estado if row["estado_asistencia"] == "tarde"), []),
            "ausentes": next((row["alumnos"] for row in totales_estado if row["estado_asistencia"] == "ausente"), []),
            }

        totales_dia_actual["alumnos_por_estado"] = alumnos_por_estado

        # Calcular estadísticas acumuladas con funciones de pyspark
        conteos_diarios = df_asignatura.groupBy(
            "estado_asistencia", date_format(col("timestamp"), "yyyy-MM-dd").alias("fecha")
        ).agg(count("alumno").alias("total_diario"))

        estadisticas_acumuladas = conteos_diarios.groupBy("estado_asistencia").agg(
            mean("total_diario").alias("media"),
            stddev("total_diario").alias("desviacion")
        )

        # Imprimir cálculos de media y desviación por asignatura
        print(f"\nCálculos para la asignatura: {asignatura}")
        estadisticas_acumuladas.show()
        #Guardar en un diccionario las esatdisticas para cada estado de asistencia
        #Usamos diccionario por comaptibilidad con JSON y organización sencilla entre clave y valor
        estadisticas_dict = estadisticas_acumuladas.collect()
        estadisticas_final = {
            "media_presentes": next((row["media"] for row in estadisticas_dict if row["estado_asistencia"] == "presente"), 0),
            "desviacion_presentes": next((row["desviacion"] for row in estadisticas_dict if row["estado_asistencia"] == "presente"), 0),
            "media_tarde": next((row["media"] for row in estadisticas_dict if row["estado_asistencia"] == "tarde"), 0),
            "desviacion_tarde": next((row["desviacion"] for row in estadisticas_dict if row["estado_asistencia"] == "tarde"), 0),
            "media_ausentes": next((row["media"] for row in estadisticas_dict if row["estado_asistencia"] == "ausente"), 0),
            "desviacion_ausentes": next((row["desviacion"] for row in estadisticas_dict if row["estado_asistencia"] == "ausente"), 0),
        }

        # Generar forecast por asignatura
        forecast = generar_forecast_por_asignatura(df_asignatura)

        # Imprimir resultados del forecast
        print(f"Forecast para la asignatura '{asignatura}': {forecast}")

        # Construir JSON para la asignatura
        data_json.append({
            "asignatura": asignatura,
            "totales_dia_actual": totales_dia_actual,
            "estadisticas_acumuladas": estadisticas_final,
            "forecast_dia_siguiente": forecast,
        })

    # ESCRIBIR json
    with open(json_output, "w", encoding="utf-8") as f:
        json.dump(data_json, f, ensure_ascii=False, indent=4)
    print(f"\nArchivo JSON generado en: {json_output}")

