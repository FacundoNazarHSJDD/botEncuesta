import pyodbc
import smtplib
from email.mime.text import MIMEText


diccionario_querys={"nombre_1";"query_1",
                    "nombre_2";"query_2",
                    "nombre_3";"query_3",
                    "nombre_4";"query_4",
                    "nombre_5";"query_5"}
# Función para conectar a la base de datos
def conectar_bd():
    server = '10.0.0.235'
    database = 'DW'
    username = 'Scheduler_02'
    password = 'Hospi21*talDw02'

    connection_string = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    connection = pyodbc.connect(connection_string)
    return connection

# Función para obtener el diccionario de links
def obtener_diccionario_links(connection):
    cursor = connection.cursor()

    query_links = "SELECT * FROM DW.dbo.Dw_Links_Encuestas"
    cursor.execute(query_links)

    diccionario_links = {}
    for row in cursor.fetchall():
        diccionario_links[row.Link] = row.ID_Encuesta_Link
        print(diccionario_links)
    return diccionario_links

# Función para enviar correos electrónicos
def enviar_correos(diccionario_links, diccionario_querys, conexion):
    for nombre, query in diccionario_querys.items():
        lista_correos = []
        cursor = conexion.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            lista_correos.append(row.MAIL_PACIENTE)
        
        if nombre in diccionario_links:
            link_form = diccionario_links[nombre]
            enviar_correo_individual(lista_correos, link_form)

def enviar_correo_individual(lista_correos, link_form):
    server = smtplib.SMTP('smtp.tu_servidor_smtp.com', 587)
    server.starttls()
    server.login('tu_correo@gmail.com', 'tu_contraseña')

    for correo in lista_correos:
        msg = MIMEText(f"""
            <div class='row' style='text-align: justify'>Estimado paciente</div class='row'><br>
            <div class='row' style='text-align: justify'>Con el fin de conocer su experiencia respecto a nuestra atención, queremos realizarle algunas preguntas. La misma tomará 1 minuto y los datos recabados son con fines estadísticos.</div class='row'><br>
            <div class='row' style='text-align: justify'>Su colaboración nos ayudará a brindar un mejor servicio. A continuación le indicamos el link donde podrá realizar la <a href='{link_form}'>ENCUESTA</a></div class='row'><br>
            <div class='row' style='text-align: justify'>Desde ya, agradecemos su tiempo y predisposición.</div class='row'><br>
            <div class='row' style='text-align: justify'>Cordialmente.</div class='row'><div class='row' style='text-align: justify'><p style='font-weight:bold'>Servicio al Cliente</p></div class='row'><div class='row' style='text-align: justify'>Departamento de Calidad</div class='row'>
        """, 'html')
        msg['Subject'] = 'Asunto del correo'
        msg['From'] = 'tu_correo@gmail.com'
        msg['To'] = correo

        server.sendmail('tu_correo@gmail.com', correo, msg.as_string())

    server.quit()

# Ejecutar el script
try:
    conexion = conectar_bd()
    diccionario_links = obtener_diccionario_links(conexion)
    envio_correos = enviar_correos(diccionario_links,diccionario_querys,conexion)
    
finally:
    if 'conexion' in locals():
        conexion.close()
