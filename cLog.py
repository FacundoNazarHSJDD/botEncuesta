import os
import logging
import datetime
class Log:
    def __init__(self):
        self.fecha_actual = datetime.datetime.now().strftime('%d-%m-%Y_%H-%M-%S')
        self.log_dir = 'Logs'
        self.log_nombre = f'{self.log_dir}/log_{self.fecha_actual}.log'

        # Crear la carpeta si no existe
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        
        # Configurar el archivo de log
        logging.basicConfig(filename=self.log_nombre, format='%(levelname)s (%(asctime)s) - (Line: %(lineno)d [%(filename)s]): %(message)s',
                            datefmt='%d/%m/%Y %I:%M:%S %p',
                            level=logging.INFO)
    