import os
import json

class Configuracion:
    @staticmethod
    def cargar():
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, 'config.json')

        try:
            with open(config_path) as archivo_config:
                configuracion = json.load(archivo_config)
            return configuracion
        except Exception as e:
            print(f"Error al cargar el archivo de configuración: {e}")
            return None
