import os 
import pandas as pd
from pipelines.api_ingestion import descargar_bronze_desde_s3
from pipelines.transformation import procesar_y_validar
from utils.aws_connections import obtener_opciones_s3, obtener_nombre_bucket

def ejecutar_pipeline():
    """
    Función principal que orquesta el flujo de la Arquitectura Medallón.
    1. Lee de S3 (Bronze)
    2. Valida en memoria RAM (Pydantic)
    3. Escribe en Local y en S3 (Silver)
    """
    print("Iniciando Pipeline de Datos industriales.")

    archivo_raw = "data.json"

    # 1. Extracción
    try:
        datos_raw = descargar_bronze_desde_s3(archivo_raw)
        print(f"Se descargaron {len(datos_raw)} registros crudos desde S3.")
    except Exception as e:
        print(f"Falló la extracción: {e}")
        return
    
    # 2. Transformación y Validación
    df_silver = procesar_y_validar(datos_raw)

    if df_silver.empty:
        print("El DataFrame está vacío. Todos los registros fueron rechazados.")
        return 
    
    print(f"Transformación exitosa. DataFrame final tiene {len(df_silver)} filas.")

    # 3. Carga
    nombre_archivo_salida = "telematria_limpia.parquet"

    # 3.1 Guardar en Local (para revisión)
    ruta_local = f"silver/{nombre_archivo_salida}"
    os.makedirs("silver", exist_ok = True) # crea la carpeta si no existe

    print(f"Guardando archivo Parquet en disco local: {ruta_local}.")

    df_silver.to_parquet(ruta_local, index = False, engine = 'pyarrow')

    # 3.2 Guardar en AWS S3 (Para producción)
    opciones_aws = obtener_opciones_s3()
    bucket = obtener_nombre_bucket()
    ruta_s3_silver = f"s3://{bucket}/silver/{nombre_archivo_salida}"
    
    print(f"Subiendo archivo Parquet a AWS S3: {ruta_s3_silver}.")

    try:
        # Pandas usa s3fs y pyarrow por debajo para escribir directo en la nube
        df_silver.to_parquet(
            ruta_s3_silver,
            storage_options = opciones_aws,
            index = False,
            engine = 'pyarrow'
        )
    except Exception as e:
        print(f"Error al intentar guardar en AWS S3: {e}.")

# Punto de entrada de Python
if __name__ == "__main__":
    ejecutar_pipeline()