import os
import unidecode
import pandas as pd
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from smtplib import SMTPException
import cCorreo, cLog
import logging

class Encuesta:
    def __init__(self, conexion, configuracion, diccionario_querys, diccionario_links, env):
        # Iniciar el logger
        self.logger = cLog.Log()
        logging.info('Inicializando la clase Facturas')
        logging.info('Instacia Log')
        
        self.conexion = conexion
        self.configuracion = configuracion
        self.diccionario_querys = diccionario_querys
        self.diccionario_links = diccionario_links
        self.env = env
        self.lista_rebotados = []
        self.tiempo_inicio = time.time()
        self.contador = 0

    def quitar_tildes(self, input_str):
        return unidecode.unidecode(input_str)

    def obtener_iniciales_mayusculas_y_minusculas(self, cadena):
        palabras = cadena.split()
        iniciales = [palabra[:1].upper() + palabra[1:].lower() for palabra in palabras]
        return ' '.join(iniciales)

    def enviar_correos(self):
        for unidad_negocio, query in self.diccionario_querys.items():
            lista_pacientes = []
            logging.info(f"{unidad_negocio}")
            
            cursor = self.conexion.cursor()
            cursor.execute(query)
            
            for row in cursor.fetchall():
                mail_paciente = self.quitar_tildes(row.MAIL_PACIENTE)
                apellido_nombre_paciente = self.quitar_tildes(row.APELLIDO_NOMBRE_PACIENTE)
                servicio_origen = self.quitar_tildes(row.SERVICIO_ORIGEN)
                lista_pacientes.append([mail_paciente, row.ID_PACIENTE, apellido_nombre_paciente, servicio_origen])

            resultados = pd.DataFrame(cursor.fetchall())
            nombre_excel = f"resultados_{unidad_negocio}.xlsx"
            resultados.to_excel(nombre_excel, index=False)

            if unidad_negocio in self.diccionario_links:
                link_form = self.diccionario_links[unidad_negocio]
                self.enviar_correo_individual(lista_pacientes, link_form, unidad_negocio)

            tiempo_transcurrido = (time.time() - self.tiempo_inicio) / 60
            if tiempo_transcurrido > 9:
                logging.info("El programa ha excedido el tiempo máximo de ejecución.")
                self.enviar_rebotados()
                logging.info("Envió mail de rebotados")
                break

        logging.info("Terminó de enviar encuestas a pacientes")

    def enviar_correo_individual(self, lista_pacientes, link_form, unidad_negocio):
        correo = cCorreo.Correo(self.configuracion)
        correo.conectar()

        script_path = os.path.dirname(os.path.abspath(__file__))
        rutaImagenHeader = os.path.join(script_path, "Header.png")
        rutaImagenFooter = os.path.join(script_path, "Footer.png")

        for paciente in lista_pacientes:
            botonHtml = f"""
                <table style='width: 100%; height: 100%;'>
                    <tr>
                        <td style='text-align: center; vertical-align: middle;'>
                            <a href='{link_form}' style='display: inline-block; width: 160px; height: 30px; padding: 5px 10px; background-color: #003365; color: white; font-size: 15px; text-decoration: none; border-radius: 15px; line-height: 30px;'>Encuesta</a>
                        </td>
                    </tr>
                </table>
            """
            HeaderHtml = f"<center><img src='cid:imagen' style='width: 560px;'></center>"
            FooterHtml = f"<center><img src='cid:imagen2' style='width: 560px;'></center>"

            self.mensaje = f"""
                <html>
                    <style>
                        body {{
                            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                            font-size: 11.25px;
                        }}
                    </style>
                    {HeaderHtml}
                    <body>
                        <br><br><center>¡Hola {self.obtener_iniciales_mayusculas_y_minusculas(paciente[2])}!<br><br>
                        ¿Nos ayudás a conocerte y brindarte un mejor servicio?<br>
                        Te invitamos a calificar tu experiencia en {self.obtener_iniciales_mayusculas_y_minusculas(paciente[3])}<br><br>
                        {botonHtml}<br>
                        Te toma 1 minuto y los datos recabados son con fines estadísticos.<br><br>
                        ¡Gracias por tu tiempo!</center><br><br>
                        <br><br>
                        {FooterHtml}
                    </body>
                </html>
            """
                    
            msg = MIMEMultipart()
            msg.attach(MIMEText(self.mensaje, 'html'))

            with open(rutaImagenHeader, 'rb') as file:
                imagen_adjunta = MIMEImage(file.read(), name=os.path.basename(rutaImagenHeader))
                imagen_adjunta.add_header('Content-ID', '<imagen>')
                msg.attach(imagen_adjunta)

            with open(rutaImagenFooter, 'rb') as file:
                imagen_firma_adjunta = MIMEImage(file.read(), name=os.path.basename(rutaImagenFooter))
                imagen_firma_adjunta.add_header('Content-ID', '<imagen2>')
                msg.attach(imagen_firma_adjunta)

            msg['Subject'] = 'Encuesta'
            msg['From'] = self.configuracion['email']['email_address']
            msg['To'] = paciente[0]
                        
            tiempo_transcurrido = (time.time() - self.tiempo_inicio) / 60
            
            if tiempo_transcurrido > 9:
                logging.info("El programa ha excedido el tiempo máximo de ejecución.")
                break
                
            try:
                correo.enviar(msg.as_string().encode('utf-8'), paciente[0])
                self.contador += 1
                logging.info(f"{paciente[0]}, contador de mails enviados: {self.contador}. Tiempo transcurrido: {tiempo_transcurrido}")
                self.insertar_registro_encuestas(paciente, unidad_negocio)
                logging.info(f"inserta {paciente} - {unidad_negocio}")
                # Añadir la pausa de 4 segundos entre envíos
                time.sleep(4)

            except SMTPException as e:
                self.insertar_registro_encuestas(paciente, unidad_negocio)
                logging.info(f"inserta {paciente} - {unidad_negocio}")
                logging.info(f"error en el envio: {e}")
                error_message = str(e)
                
                if "501" in error_message and "5.1.3 Invalid address" in error_message:
                    logging.info("Error 501: Invalid address format.")
                    self.lista_rebotados.append([paciente, error_message])
                elif "510" in error_message or "511" in error_message:
                    logging.info("One of the addresses in your TO, CC or BBC line doesn't exist. Check again your recipients' accounts and correct any possible misspelling.")
                    self.lista_rebotados.append([paciente, error_message])
                elif "512" in error_message:
                    logging.info("Check again all your recipients' addresses: there will likely be an error in a domain name (like mail@domain.coom instead of mail@domain.com)")
                    self.lista_rebotados.append([paciente, error_message])
                else:
                    logging.info(f"SMTP Error: {error_message}")

        correo.desconectar()

    def insertar_registro_encuestas(self, paciente, unidad_negocio):
        try:
            cursor = self.conexion.cursor()
            fecha_envio = 'GETDATE()'

            query_insert = f"""
                INSERT INTO DW.dbo.Dw_Registro_Encuestas
                (ID_Paciente, Mail, Paciente, Servicio, Ambiente, Encuesta, Fecha_Envio)
                VALUES ({paciente[1]}, '{paciente[0]}', '{paciente[2]}', '{paciente[3]}', '{self.env}', '{unidad_negocio}', {fecha_envio});
            """

            cursor.execute(query_insert)
            self.conexion.commit()
        except Exception as e:
            logging.info(f"Error al insertar registro de encuestas: {e}")

    def enviar_rebotados(self):
        correo = cCorreo.Correo(self.configuracion)
        correo.conectar()

        mensaje_rebotados = f"""
            <html>
                <style>
                    body {{
                        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                        font-size: 11.25px;
                    }}
                </style>
                <body>
                    <br><br><center>¡Hola!<br><br>
                    La siguiente lista no pudo recibir el correo de encuesta <br>
                    de satisfacción por inexistencia del correo o error de tipeo.<br>
                    {self.lista_rebotados}<br><br>
                    ¡Gracias por tu tiempo!</center><br><br>
                    <br><br>
                </body>
            </html>
        """
        msg = MIMEMultipart()
        msg.attach(MIMEText(mensaje_rebotados, 'html'))
        msg['Subject'] = 'Revisión de mails de Encuesta'
        msg['From'] = self.configuracion['email']['email_address']
        msg['To'] = "facundo.nazar@sanjuandedios.org.ar"
        msg['Cc'] = "bi@sanjuandedios.org.ar"
        
        if len(self.lista_rebotados)>0:    
            try:
                correo.enviar(msg.as_string().encode('utf-8'), msg['To'])
            except SMTPException as e:
                logging.info(f"SMTP Error: {str(e)}")
        else:
            pass
        correo.desconectar()
