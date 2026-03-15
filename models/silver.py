from pydantic import model_validator
from models.bronze import TelemetriaBronce

class TelemetriaPlata(TelemetriaBronce):
    # contrato de datos limpios
    # hereda de Bronce, limpia el texto y aplica validaciones físicas
    temperatura_clean: float = 0.0
    rpm_clean: int = 0
    estado_alerta: bool = False

    @model_validator(mode = 'after')
    def aplicar_reglas_negocio(self) -> 'TelemetriaPlata':
        # 1. Limpieza de temperaturas
        try:
            temp_str = self.temperatura_raw.replace("C", "").strip()
            self.temperatura_clean = float(temp_str)
        except ValueError:
            raise ValueError(f"Error de formato en temperatura: {self.temperatura_raw}")
        
        # 2. Vallidación de la Física: Rango operativo de temperatura
        if self.temperatura_clean > 1000:
            raise ValueError("Temperatura irreal (>1000°C). Sensor dañado.")
        
        # 3. Limpieza de RPM
        try:
            rpm_str = str(self.rpm_raw).replace("rpm", "").strip()
            self.rpm_clean = int(float(rpm_str)) # Agregamos float por si viene "1150.0"
        except ValueError:
            raise ValueError(f"Error de formato en RPM: {self.rpm_clean}")
        
        # 4. Validación de la física: RPM no pueden ser negativos
        if self.rpm_clean < 0:
            raise ValueError("Física imposible: Las RPM no pueden ser negativas.")
        
        # 5. Lógica de Negocio: Alerta preventiva
        self.estado_alerta = self.temperatura_clean > 90

        return self