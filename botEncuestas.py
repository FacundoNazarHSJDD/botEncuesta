import pyodbc
import smtplib
from email.mime.text import MIMEText

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

# Función para ejecutar la consulta por cada link y obtener la lista de correos
def obtener_lista_correos(connection, diccionario_links):
    lista_correos = []

    for link, id_link in diccionario_links.items():
        query_correo = f"SELECT MAIL_PACIENTE FROM tu_tabla WHERE ID_Encuesta_Link = {id_link}"
        
        cursor = connection.cursor()
        cursor.execute(query_correo)

        for row in cursor.fetchall():
            lista_correos.append(row.MAIL_PACIENTE)

    return lista_correos

# Función para enviar correos electrónicos
def enviar_correos(lista_correos, link_form):
    a=0# ... (mismo código de la función anterior)

# Ejecutar el script
try:
    conexion = conectar_bd()
    diccionario_links = obtener_diccionario_links(conexion)
    lista_correos = obtener_lista_correos(conexion, diccionario_links)
    link_form = diccionario_links['https://forms.office.com/r/f17NFWVGbG']  # Reemplaza con el link correcto
    enviar_correos(lista_correos, link_form)
finally:
    if 'conexion' in locals():
        conexion.close()
