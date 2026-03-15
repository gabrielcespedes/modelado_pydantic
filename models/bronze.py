from pydantic import BaseModel, ConfigDict
from typing import Any

class TelemetriaBronce(BaseModel):
    # contrato de datos crudos
    model_config = ConfigDict(arbitrary_types_allowed = True)

    id_sensor: str
    timestamp_raw: Any
    temperatura_raw: str
    rpm_raw: str
