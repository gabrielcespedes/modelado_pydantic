import datetime

def enviar_alerta_cuarentena(id_sensor: str, motivo_error: str):
    # Simula el envío de una alerta cuando un dato es bloqueado por Pydantic
    hora_actual = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mensaje = f"[ALERTA {hora_actual}] Dato bloqueado -> Sensor: {id_sensor}"

    # usaríamos una API para enviar a por ejemplo Slack, Teams
    print(f"\033[91m{mensaje}\033[0m")