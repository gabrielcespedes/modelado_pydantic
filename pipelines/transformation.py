import pandas as pd
from pydantic import ValidationError

from models.silver import TelemetriaPlata
from utils.alerts import enviar_alerta_cuarentena

def procesar_y_validar(datos_raw: list[dict]) -> pd.DataFrame:
    # toma los datos crudos, los pasa por el contrato Pydantic, aísla los errores y devuelve un DataFrame limpio
    registros_validos = []
    print("Iniciando validación fila por fila con Pydantic.")

    print(f"\n--- [DEBUG] REVISANDO DATOS RECIBIDOS DE AWS ---")
    print(f"Total de registros recibidos: {len(datos_raw)}")
    
    # Miramos el primer registro con lupa
    if len(datos_raw) > 0:
        primer_registro = datos_raw[0]
        print(f"Estructura del primer registro:")
        print(f"Tipo de dato: {type(primer_registro)}")
        print(f"Contenido: {primer_registro}")
        print(f"Llaves disponibles: {primer_registro.keys()}")
    print(f"-----------------------------------------------\n")

    for registro in datos_raw:
        try:
            # 1. Pydantic evalúa la fila
            dato_clean = TelemetriaPlata(**registro)
            # 2. Si es válido, guardamos el diccionario con los datos limpios y tipados
            registros_validos.append(dato_clean.model_dump())
        except ValidationError as error_pydantic:
            # 3. Capturamos el error y simulamos envío a cuarentena
            id_fallido = registro.get("id_sensor", "DESCONOCIDO")
            # Extraemos el mensaje de error específico
            motivo = error_pydantic.errors()[0]['msg']

            enviar_alerta_cuarentena(id_sensor = id_fallido, motivo_error = motivo)
            continue
    
    total_raw = len(datos_raw)
    total_validos = len(registros_validos)

    print(f"Validación terminada. Filas perfectas: {total_validos} | Filas en cuarentena: {total_raw - total_validos}")

    # 4. Convertimos los objetos garantizados en un DataFrame masivo
    print("Generando DataFrame de la capa silver.")
    df_silver = pd.DataFrame(registros_validos)

    return df_silver