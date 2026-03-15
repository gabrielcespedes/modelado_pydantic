import os
from dotenv import load_dotenv

def obtener_opciones_s3() -> dict:
    """
    Lee las variables de entorno.
    Retorna un diccionario con las credenciales formateadas
    exactamente como Pandas las necesita para escribir en S3.
    """
    # carga las variables de entorno desde el archivo .env a la memoria RAM
    load_dotenv()

    key = os.getenv('AWS_ACCESS_KEY_ID')
    secret = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('AWS_REGION', 'us-east-1')  # Capturamos la región

    if not key or not secret:
        raise ValueError("Credenciales de AWS no encontradas en .env")

    # Pandas usa un parámetro llamado 'storage_options' para autenticarse
    storage_options = {
        'key': key,
        'secret': secret,
        # 'anon': False, # no intenta entrar como anónimo
        'client_kwargs': {
            'region_name': region
        },
        # 'config_kwargs': {
        #     'signature_version': 's3v4' # forzar el protocolo de seguridad moderno de AWS
        # }
    }

    return storage_options

def obtener_nombre_bucket() -> str:
    # retorna el nombre del bucket destino
    load_dotenv()
    return os.getenv('S3_BUCKET_NAME', 'datalakeindustrialc')
