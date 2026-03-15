import pandas as pd
from utils.aws_connections import obtener_opciones_s3, obtener_nombre_bucket

def descargar_bronze_desde_s3(nombre_archivo: str) -> list[dict]:
    # se conecta a AWS S3, lee el archivo JSON de la capa Bronze
    # y lo convierte en una lista de diccionarios para que Pydantic lo procese

    # 1. Obtenemos las llaves secretas y el nombre del bucket desde .env
    opciones_aws = obtener_opciones_s3()
    bucket = obtener_nombre_bucket()

    # 2. Armamos la ruta exacta en la nube
    ruta_s3 = f"s3://{bucket}/bronze/{nombre_archivo}"
    print(f"Conectando a AWS: Leyendo archivos desde: {ruta_s3}")

    try:
        # 3. Pandas lee direcetamente desde S3 gracias a las librerías s3fs y boto3
        df_raw = pd.read_json(ruta_s3, storage_options = opciones_aws, orient = 'records')

        # 4. Convertimos el DataFrame a una lista de diccionarios (formato ideal para Pydantic)
        datos_raw = df_raw.to_dict(orient = 'records')
        return datos_raw
    
    except Exception as e:
        raise ConnectionError(f"Error al intentar leer S3: {e}")