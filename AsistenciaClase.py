import json
import warnings
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_format, max, count, collect_list, mean, stddev
)
from statsmodels.tsa.arima.model import ARIMA
from pmdarima import auto_arima  
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

# Función para calcular métricas diarias, media y desviación por estados y asignaturas
# y posterior guardado en JSON junto con el forecast
def calcular_metricas_y_forecast_json(df, json_output):
    # Obtener asignaturas una única vez
    asignaturas_df = df.select("asignatura").distinct()
    # Lista para almacenar resultados
    data_json = []

    # Procesar cada asignatura
    for asignatura_row in asignaturas_df.collect():
        # ¿Por qué collect? Porque los datos de PySpark no comprometen la memoria
        asignatura = asignatura_row["asignatura"]

        # Datos por asignatura
        df_asignatura = df.filter(col("asignatura") == asignatura)

        # Obtener el último día de registros para calcular datos del día actual
        ultimo_dia = df_asignatura.agg(
            max(date_format(col("timestamp"), "yyyy-MM-dd")).alias("ultimo_dia")
        ).first()["ultimo_dia"]

        # Filtrar registros del último día
        df_ultimo_dia = df_asignatura.filter(
            date_format(col("timestamp"), "yyyy-MM-dd") == ultimo_dia
        )

        # Calcular totales del último día por agrupación
        totales_estado = df_ultimo_dia.groupBy("estado_asistencia").agg(
            count("alumno").alias("total"),
            collect_list("alumno").alias("alumnos")
        ).collect()

        # Crear diccionario para totales del día actual
        totales_dia_actual = {
            "presentes": next(
                (row["total"] for row in totales_estado if row["estado_asistencia"] == "presente"),
                0
            ),
            "tarde": next(
                (row["total"] for row in totales_estado if row["estado_asistencia"] == "tarde"),
                0
            ),
            "ausentes": next(
                (row["total"] for row in totales_estado if row["estado_asistencia"] == "ausente"),
                0
            ),
        }

        alumnos_por_estado = {
            "presentes": next(
                (row["alumnos"] for row in totales_estado if row["estado_asistencia"] == "presente"),
                []
            ),
            "tarde": next(
                (row["alumnos"] for row in totales_estado if row["estado_asistencia"] == "tarde"),
                []
            ),
            "ausentes": next(
                (row["alumnos"] for row in totales_estado if row["estado_asistencia"] == "ausente"),
                []
            ),
        }

        totales_dia_actual["alumnos_por_estado"] = alumnos_por_estado

        # Calcular estadísticas acumuladas con funciones de PySpark
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

        # Guardar en un diccionario las estadísticas para cada estado de asistencia
        estadisticas_dict = estadisticas_acumuladas.collect()
        estadisticas_final = {
            "media_presentes": next(
                (row["media"] for row in estadisticas_dict if row["estado_asistencia"] == "presente"),
                0
            ),
            "desviacion_presentes": next(
                (row["desviacion"] for row in estadisticas_dict if row["estado_asistencia"] == "presente"),
                0
            ),
            "media_tarde": next(
                (row["media"] for row in estadisticas_dict if row["estado_asistencia"] == "tarde"),
                0
            ),
            "desviacion_tarde": next(
                (row["desviacion"] for row in estadisticas_dict if row["estado_asistencia"] == "tarde"),
                0
            ),
            "media_ausentes": next(
                (row["media"] for row in estadisticas_dict if row["estado_asistencia"] == "ausente"),
                0
            ),
            "desviacion_ausentes": next(
                (row["desviacion"] for row in estadisticas_dict if row["estado_asistencia"] == "ausente"),
                0
            ),
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

    # Escribir JSON
    with open(json_output, "w", encoding="utf-8") as f:
        json.dump(data_json, f, ensure_ascii=False, indent=4)
    print(f"\nArchivo JSON generado en: {json_output}")

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

# Configuración y rutas
spark = SparkSession.builder.appName("AsistenciaClaseForecast").getOrCreate()
# Rutas para archivos entrada y salida
csv_input = "asistencia_clase_modificada.csv"
csv_nulos = "nulos.csv"
csv_filtrado = "filtrado.csv"
json_output = "asistencia_forecast.json"
