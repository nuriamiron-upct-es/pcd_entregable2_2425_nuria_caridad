import asyncio
import statistics
from datetime import datetime, timedelta
from collections import deque
from gms_to_olc import gms_a_olc
import random

# ---------- SINGLETON: GESTOR DEL SISTEMA ----------
class LogisticaSystem:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LogisticaSystem, cls).__new__(cls)
            cls._instance.camiones = {}
        return cls._instance

    def registrar_camion(self, id_camion):
        self.camiones[id_camion] = deque()

    def recibir_dato(self, id_camion, dato):
        try:
            if id_camion not in self.camiones:
                self.registrar_camion(id_camion)
            self.camiones[id_camion].append(dato)
            self.filtrar_datos(id_camion)
        except Exception as e:
            print(f"Error al recibir dato del camión {id_camion}: {e}")

    def filtrar_datos(self, id_camion):
        try:
            ahora = datetime.now()
            self.camiones[id_camion] = deque([
                d for d in self.camiones[id_camion]
                if datetime.fromisoformat(d["timestamp"]) >= ahora - timedelta(seconds=60)
            ])
        except Exception as e:
            print(f"Error al filtrar datos del camión {id_camion}: {e}")

    def obtener_datos(self, id_camion):
        try:
            return list(self.camiones.get(id_camion, []))
        except Exception as e:
            print(f"Error al obtener datos del camión {id_camion}: {e}")
            return []

# ---------- ESTRATEGIAS DE PROCESAMIENTO ----------
def calcular_estadisticas(datos):
    try:
        temps = list(map(lambda d: d['t'], datos))
        hums = list(map(lambda d: d['h'], datos))
        return {
            'temp_media': statistics.mean(temps),
            'temp_std': statistics.stdev(temps) if len(temps) > 1 else 0.0,
            'hum_media': statistics.mean(hums),
            'hum_std': statistics.stdev(hums) if len(hums) > 1 else 0.0
        }
    except Exception as e:
        print(f"Error al calcular estadísticas: {e}")
        return {}

def temperatura_fuera_rango(dato, umbral):
    try:
        return dato['t'] > umbral
    except Exception as e:
        print(f"Error al verificar temperatura fuera de rango: {e}")
        return False

def variacion_significativa(datos):
    try:
        ultimos_30s = list(filter(lambda d: datetime.fromisoformat(d['timestamp']) >= datetime.now() - timedelta(seconds=30), datos))
        if len(ultimos_30s) < 2:
            return False
        temps = list(map(lambda d: d['t'], ultimos_30s))
        hums = list(map(lambda d: d['h'], ultimos_30s))
        return max(temps) - min(temps) > 2 or max(hums) - min(hums) > 2
    except Exception as e:
        print(f"Error al verificar variación significativa: {e}")
        return False

# ---------- CONVERSIÓN DE COORDENADAS ----------
def convertir_localizacion(lo_gms, la_gms):
    try:
        return gms_a_olc(la_gms, lo_gms)
    except Exception as e:
        print(f"Error al convertir localización: {e}")
        return ""

# ---------- convertir_localizacion(lo_gms, la_gms) ----------
async def procesar_dato(id_camion, dato, umbral=25):
    try:
        sistema = LogisticaSystem()
        sistema.recibir_dato(id_camion, dato)
        datos = sistema.obtener_datos(id_camion)
        stats = calcular_estadisticas(datos)
        print(f"Estadísticas para {id_camion}:", stats)
        if temperatura_fuera_rango(dato, umbral):
            print(f"Camión {id_camion} supera el umbral de temperatura: {dato['t']}°C")
        await asyncio.create_task(verificar_variacion(id_camion, datos))
    except Exception as e:
        print(f"Error al procesar dato del camión {id_camion}: {e}")

async def verificar_variacion(id_camion, datos):
    try:
        if variacion_significativa(datos):
            print(f"Camión {id_camion} tiene variación significativa de temperatura o humedad en los últimos 30s")
    except Exception as e:
        print(f"Error al verificar variación del camión {id_camion}: {e}")

# ---------- SIMULACIÓN DE ENVÍO DE DATOS ----------
def generar_dato_aleatorio():
    try:
        ahora = datetime.now().isoformat()
        temp = round(random.uniform(5, 30), 2)
        hum = round(random.uniform(30, 80), 2)
        lon_gms = (1, 0, 0.0, 'W')  # fijo para ejemplo
        lat_gms = (38, 0, 0.0, 'N')
        olc = convertir_localizacion(lon_gms, lat_gms)
        return {
            'timestamp': ahora,
            't': temp,
            'h': hum,
            'localizacion': olc
        }
    except Exception as e:
        print(f"Error al generar dato aleatorio: {e}")
        return {}

async def main():
    id_camion = 'C-1234'
    for _ in range(10):  # simula 10 envíos
        dato = generar_dato_aleatorio()
        await procesar_dato(id_camion, dato)
        await asyncio.sleep(5)

if __name__ == '__main__':
    asyncio.run(main())
