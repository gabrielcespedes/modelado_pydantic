import os
import boto3
import s3fs
import pandas as pd
from dotenv import load_dotenv

def ejecutar_diagnostico():
    print("--- INICIANDO DIAGNÓSTICO INTEGRAL DE CONEXIÓN A AWS ---")

    # 1. Prueba de Variables de Entorno (Local)
    load_dotenv()
    key = os.getenv('AWS_ACCESS_KEY_ID')
    secret = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('AWS_REGION', 'us-east-1')
    bucket_name = os.getenv('S3_BUCKET_NAME', 'datalakeindustrialc')
    
    # Archivo objetivo para las pruebas profundas
    archivo_key = 'bronze/data.json'
    ruta_completa = f"s3://{bucket_name}/{archivo_key}"

    print(f"\nPaso 1: Leyendo credenciales locales...")
    if not key:
        print("ERROR: Llave pública no encontrada. Python NO está leyendo tu archivo .env.")
        print("Solución: Verifica que el archivo se llame exactamente '.env' y esté en la raíz.")
        return
    else:
        print(f"Llave capturada exitosamente (Inicia con: {key[:5]}...)")

    # 2. Prueba de Permisos IAM Básicos (Listar)
    print(f"\nPaso 2: Tocando la puerta del bucket '{bucket_name}'...")
    try:
        s3 = boto3.client('s3', aws_access_key_id=key, aws_secret_access_key=secret, region_name=region)
        archivos = s3.list_objects_v2(Bucket=bucket_name)
        print("¡Permisos IAM correctos! AWS te reconoció y permite listar (ListBucket).")
        
        # 3. Prueba de Existencia de Archivos
        print("\nPaso 3: Listando el contenido real de tu bucket:")
        if 'Contents' in archivos:
            for obj in archivos['Contents']:
                print(f"  - s3://{bucket_name}/{obj['Key']}")
        else:
            print("El bucket está vacío.")
            
    except Exception as e:
        print(f"AWS RECHAZÓ EL ACCESO (Forbidden) en Paso 2:")
        print(f"Detalle técnico: {e}")
        return # Si no podemos ni listar, no tiene sentido seguir

    # 4. Prueba Profunda: Permiso de Lectura (GetObject)
    print(f"\nPaso 4: Intentando DESCARGAR el archivo ({archivo_key}) con Boto3...")
    try:
        respuesta = s3.get_object(Bucket=bucket_name, Key=archivo_key)
        # Leemos solo un pedacito para confirmar que funciona
        contenido = respuesta['Body'].read(20).decode('utf-8')
        print("¡Éxito! Boto3 tiene permiso para leer el contenido (GetObject).")
    except Exception as e:
        print(f"ERROR BOTO3: AWS te deja ver la lista de archivos, pero NO leerlos.")
        print(f"Detalle: {e}")
        return

    # 5. Prueba Profunda: Traductor s3fs
    print(f"\nPaso 5: Intentando conectar a través del traductor s3fs...")
    try:
        fs = s3fs.S3FileSystem(key=key, secret=secret, client_kwargs={'region_name': region})
        with fs.open(ruta_completa, 'rb') as f:
            f.read(10)
        print("¡Éxito! s3fs logra entender la ruta y la región.")
    except Exception as e:
        print(f"ERROR S3FS: El traductor falló. Detalle: {e}")
        return

    # 6. Prueba Profunda: Pandas DataFrame
    print(f"\nPaso 6: Intentando inyectar el JSON a Pandas...")
    try:
        opciones = {'key': key, 'secret': secret, 'client_kwargs': {'region_name': region}}
        df = pd.read_json(ruta_completa, storage_options=opciones, orient='records')
        print("¡Éxito Total! Pandas logró crear el DataFrame. El problema no era de conexión.")
        print(df.head(2))
    except Exception as e:
        print(f"ERROR PANDAS: Conexión perfecta, pero Pandas falló al leer el archivo.")
        print(f"Detalle: {e}")

if __name__ == '__main__':
    ejecutar_diagnostico()