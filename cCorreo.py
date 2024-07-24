import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

class Correo:
    def __init__(self, configuracion):
        self.configuracion = configuracion
        self.server = None

    def conectar(self):
        try:
            self.server = smtplib.SMTP(self.configuracion['email']['smtp_server'], self.configuracion['email']['smtp_port'])
            self.server.starttls()
            self.server.login(self.configuracion['email']['email_address'], self.configuracion['email']['email_password'])
        except Exception as e:
            print(f"Error al conectar al servidor de correo: {e}")

    def desconectar(self):
        if self.server:
            self.server.quit()

    def enviar(self, mensaje, destinatario):
        self.server.sendmail(self.configuracion['email']['email_address'], destinatario, mensaje)
