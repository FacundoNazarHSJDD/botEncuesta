import pyodbc
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
import pandas as pd
import json
import os

def cargar_configuracion():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.json')

    try:
        with open(config_path) as archivo_config:
            configuracion = json.load(archivo_config)
        return configuracion
    except Exception as e:
        print(f"Error al cargar el archivo de configuración: {e}")
        return None

# Variable de entorno que indica el entorno de ejecución (desarrollo o producción)
env="dev"

# Diccionario que mapea el nombre de la encuesta a la consulta SQL correspondiente
diccionario_querys={
                    "Encuesta_AMA_Castelar": """SELECT
        tdp.MAIL_1 AS MAIL_PACIENTE,
        tfr.ID_PACIENTE,
        tdp.APELLIDO_NOMBRE_PACIENTE,
        tdsc.SERVICIO_ORIGEN
    FROM
        dw.dbo.TS_Fact_Recepcion tfr
    LEFT JOIN
        dw.dbo.TS_Dim_Paciente tdp ON tdp.ID_PACIENTE = tfr.ID_PACIENTE
    INNER JOIN
        dw.dbo.TS_Fact_Atencion tfa ON tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB
    LEFT JOIN
        dw.dbo.TS_Dim_Servicio_Centro tdsc ON tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO
    WHERE
        tfa.FECHA_HORA_FIN_ATE_MED BETWEEN dateadd(DD, -1, CONVERT(DATE, GETDATE())) AND CONVERT(DATE, GETDATE())
        AND LEN(tdp.MAIL_1) >= 4
        AND tfr.ANULADA_RECEPCION = 'n'
        AND tfr.ANULADA_DET_RECEPCION = 'n'
        AND tfa.ESTADO_ATENCION = 'FINALIZADA'
        AND tfa.ANULADA_DET_ATENCION = 'n'
        AND tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
        AND tdsc.UNIDAD_NEGOCIO = 'Atencion Medica Ambulatoria'
        AND tfr.ID_CENTRO = 2
        AND tfr.ID_SERVICIO_CENTRO_REALIZA NOT IN (4, 144, 153)
        AND tfr.COD_DET_PREST_RECEP_AMB IN (
            SELECT
                MIN(tfr.COD_DET_PREST_RECEP_AMB)
            FROM
                dw.dbo.TS_Fact_Recepcion tfr
            LEFT JOIN
                dw.dbo.TS_Dim_Paciente tdp ON tdp.ID_PACIENTE = tfr.ID_PACIENTE
            INNER JOIN
                dw.dbo.TS_Fact_Atencion tfa ON tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB
            LEFT JOIN
                dw.dbo.TS_Dim_Servicio_Centro tdsc ON tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO
            WHERE
                tfa.FECHA_HORA_FIN_ATE_MED BETWEEN dateadd(DD, -1, CONVERT(DATE, GETDATE())) AND CONVERT(DATE, GETDATE())
                AND LEN(tdp.MAIL_1) >= 4
                AND tfr.ANULADA_RECEPCION = 'n'
                AND tfr.ANULADA_DET_RECEPCION = 'n'
                AND tfa.ESTADO_ATENCION = 'FINALIZADA'
                AND tfa.ANULADA_DET_ATENCION = 'n'
                AND tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
                AND tdsc.UNIDAD_NEGOCIO = 'Atencion Medica Ambulatoria'
                AND tfr.ID_CENTRO = 2
                AND tfr.ID_SERVICIO_CENTRO_REALIZA NOT IN (4, 144, 153)
            GROUP BY
                tfr.ID_PACIENTE
        )
        AND tfr.ID_PACIENTE NOT IN (
            SELECT
                id_paciente
            FROM
                dw.dbo.Dw_Registro_Encuestas s
            WHERE
                CONVERT(DATE, s.Fecha_Envio) = CONVERT(DATE, GETDATE())
        )
    GROUP BY
        tfr.ID_PACIENTE,
        tdp.MAIL_1,
        datepart(w, FECHA_HORA_RECEP),
        tdp.APELLIDO_NOMBRE_PACIENTE,
        tdsc.SERVICIO_ORIGEN""",
        "Encuesta_AMA_Ramos": """SELECT
        tdp.MAIL_1 AS MAIL_PACIENTE,
        tfr.ID_PACIENTE,
        tdp.APELLIDO_NOMBRE_PACIENTE,
        tdsc.SERVICIO_ORIGEN
    FROM
        dw.dbo.TS_Fact_Recepcion tfr
    LEFT JOIN
        dw.dbo.TS_Dim_Paciente tdp ON tdp.ID_PACIENTE = tfr.ID_PACIENTE
    INNER JOIN
        dw.dbo.TS_Fact_Atencion tfa ON tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB
    LEFT JOIN
        dw.dbo.TS_Dim_Servicio_Centro tdsc ON tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO
    WHERE
        tfa.FECHA_HORA_FIN_ATE_MED BETWEEN dateadd(DD, -1, CONVERT(DATE, GETDATE())) AND CONVERT(DATE, GETDATE())
        AND LEN(tdp.MAIL_1) >= 4
        AND tfr.ANULADA_RECEPCION = 'n'
        AND tfr.ANULADA_DET_RECEPCION = 'n'
        AND tfa.ESTADO_ATENCION = 'FINALIZADA'
        AND tfa.ANULADA_DET_ATENCION = 'n'
        AND tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
        AND tdsc.UNIDAD_NEGOCIO = 'Atencion Medica Ambulatoria'
        AND tfr.ID_CENTRO = 1
        AND tfr.ID_SERVICIO_CENTRO_REALIZA NOT IN (4, 144, 153)
        AND tfr.COD_DET_PREST_RECEP_AMB IN (
            SELECT
                MIN(tfr.COD_DET_PREST_RECEP_AMB)
            FROM
                dw.dbo.TS_Fact_Recepcion tfr
            LEFT JOIN
                dw.dbo.TS_Dim_Paciente tdp ON tdp.ID_PACIENTE = tfr.ID_PACIENTE
            INNER JOIN
                dw.dbo.TS_Fact_Atencion tfa ON tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB
            LEFT JOIN
                dw.dbo.TS_Dim_Servicio_Centro tdsc ON tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO
            WHERE
                tfa.FECHA_HORA_FIN_ATE_MED BETWEEN dateadd(DD, -1, CONVERT(DATE, GETDATE())) AND CONVERT(DATE, GETDATE())
                AND LEN(tdp.MAIL_1) >= 4
                AND tfr.ANULADA_RECEPCION = 'n'
                AND tfr.ANULADA_DET_RECEPCION = 'n'
                AND tfa.ESTADO_ATENCION = 'FINALIZADA'
                AND tfa.ANULADA_DET_ATENCION = 'n'
                AND tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
                AND tdsc.UNIDAD_NEGOCIO = 'Atencion Medica Ambulatoria'
                AND tfr.ID_CENTRO = 1
                AND tfr.ID_SERVICIO_CENTRO_REALIZA NOT IN (4, 144, 153)
            GROUP BY
                tfr.ID_PACIENTE
        )
        AND tfr.ID_PACIENTE NOT IN (
            SELECT
                id_paciente
            FROM
                dw.dbo.Dw_Registro_Encuestas s
            WHERE
                CONVERT(DATE, s.Fecha_Envio) = CONVERT(DATE, GETDATE())
        )
    GROUP BY
        tfr.ID_PACIENTE,
        tdp.MAIL_1,
        datepart(w, FECHA_HORA_RECEP),
        tdp.APELLIDO_NOMBRE_PACIENTE,
        tdsc.SERVICIO_ORIGEN
""",
"Encuesta_AMU_Adultos":"""SELECT
    tdp.MAIL_1 AS MAIL_PACIENTE,
    tfr.ID_PACIENTE,
    tdp.APELLIDO_NOMBRE_PACIENTE,
    tdsc.SERVICIO_ORIGEN as SERVICIO_ORIGEN,
    tdsc.UNIDAD_NEGOCIO
FROM
    dw.dbo.TS_Fact_Recepcion tfr
LEFT JOIN dw.dbo.TS_Dim_Paciente tdp ON tdp.ID_PACIENTE = tfr.ID_PACIENTE
INNER JOIN dw.dbo.TS_Fact_Atencion tfa ON tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB
LEFT JOIN dw.dbo.TS_Dim_Servicio_Centro tdsc ON tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO
WHERE
    tfa.FECHA_HORA_FIN_ATE_MED BETWEEN DATEADD(DD, -1, CONVERT(DATE, GETDATE())) AND CONVERT(DATE, GETDATE())
    AND LEN(tdp.MAIL_1) >= 4
    AND tfr.ANULADA_RECEPCION = 'n'
    AND tfr.ANULADA_DET_RECEPCION = 'n'
    AND tfa.ESTADO_ATENCION = 'FINALIZADA'
    AND tfa.ANULADA_DET_ATENCION = 'n'
    AND tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
    AND tdsc.UNIDAD_NEGOCIO = 'Atencion Medica de Urgencias'
    AND tfr.ID_SERVICIO_CENTRO_FACTURA = 109
    AND DATEDIFF(YEAR, tdp.FECHA_NACIMIENTO_PACIENTE, GETDATE()) > 15
    AND tfr.ID_PACIENTE NOT IN (
        SELECT tfi.ID_PACIENTE
        FROM dw.dbo.TS_Fact_Internacion tfi
        LEFT JOIN dw.dbo.TS_Dim_Paciente tdp ON tdp.ID_PACIENTE = tfi.ID_PACIENTE
        WHERE tfi.FECHA_ALTA_MEDICA BETWEEN DATEADD(DD, -1, CONVERT(DATE, GETDATE())) AND CONVERT(DATE, GETDATE())
            AND LEN(tdp.MAIL_1) >= 4
            AND tfi.ANULADA_INTERNACION ='N'
            AND tfi.ID_ORIGEN_INTERNACION = 8
            AND tfi.ID_TIPO_ALTA <> 6
            AND tfi.ID_FUNCION_INTERNACION = 5
    )
    AND tfr.ID_PACIENTE NOT IN (
        SELECT id_paciente
        FROM dw.dbo.Dw_Registro_Encuestas s
        WHERE CONVERT(DATE, s.Fecha_Envio) = CONVERT(DATE, GETDATE())
    )
GROUP BY
    tfr.ID_PACIENTE,
    tdp.MAIL_1,
    DATEPART(w, FECHA_HORA_RECEP),
    tdsc.SERVICIO_ORIGEN,
    tdp.APELLIDO_NOMBRE_PACIENTE,
    tdsc.UNIDAD_NEGOCIO
""",
"Encuesta_AMU_Pediatricos":"""SELECT
    tdp.MAIL_1 AS MAIL_PACIENTE,
    tfr.ID_PACIENTE,
    tdp.APELLIDO_NOMBRE_PACIENTE,
    tdsc.SERVICIO_ORIGEN,
    tdsc.UNIDAD_NEGOCIO
FROM
    dw.dbo.TS_Fact_Recepcion tfr
LEFT JOIN dw.dbo.TS_Dim_Paciente tdp ON tdp.ID_PACIENTE = tfr.ID_PACIENTE
INNER JOIN dw.dbo.TS_Fact_Atencion tfa ON tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB
LEFT JOIN dw.dbo.TS_Dim_Servicio_Centro tdsc ON tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO
WHERE
    tfa.FECHA_HORA_FIN_ATE_MED BETWEEN DATEADD(DD, -1, CONVERT(DATE, GETDATE())) AND CONVERT(DATE, GETDATE())
    AND LEN(tdp.MAIL_1) >= 4
    AND tfr.ANULADA_RECEPCION = 'n'
    AND tfr.ANULADA_DET_RECEPCION = 'n'
    AND tfa.ESTADO_ATENCION = 'FINALIZADA'
    AND tfa.ANULADA_DET_ATENCION = 'n'
    AND tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
    AND tdsc.UNIDAD_NEGOCIO = 'Atencion Medica de Urgencias'
    AND tfr.ID_SERVICIO_CENTRO_FACTURA = 109
    AND DATEDIFF(YEAR, tdp.FECHA_NACIMIENTO_PACIENTE, GETDATE()) < 15
    AND tfr.ID_PACIENTE NOT IN (
        SELECT tfi.ID_PACIENTE
        FROM dw.dbo.TS_Fact_Internacion tfi
        LEFT JOIN dw.dbo.TS_Dim_Paciente tdp ON tdp.ID_PACIENTE = tfi.ID_PACIENTE
        WHERE tfi.FECHA_ALTA_MEDICA BETWEEN DATEADD(DD, -1, CONVERT(DATE, GETDATE())) AND CONVERT(DATE, GETDATE())
            AND LEN(tdp.MAIL_1) >= 4
            AND tfi.ANULADA_INTERNACION ='N'
            AND tfi.ID_ORIGEN_INTERNACION = 8
            AND tfi.ID_TIPO_ALTA <> 6
            AND tfi.ID_FUNCION_INTERNACION = 5
    )
    AND tfr.ID_PACIENTE NOT IN (
        SELECT id_paciente
        FROM dw.dbo.Dw_Registro_Encuestas s
        WHERE CONVERT(DATE, s.Fecha_Envio) = CONVERT(DATE, GETDATE())
    )
GROUP BY
    tfr.ID_PACIENTE,
    tdp.MAIL_1,
    DATEPART(w, FECHA_HORA_RECEP),
    tdsc.SERVICIO_ORIGEN,
    tdp.APELLIDO_NOMBRE_PACIENTE,
    tdsc.UNIDAD_NEGOCIO
""",
"Encuesta_AMU_Telefonicas":"""SELECT
    tdp.MAIL_1 AS MAIL_PACIENTE,
    tfr.ID_PACIENTE,
    tdp.APELLIDO_NOMBRE_PACIENTE,
    tdsc.SERVICIO_ORIGEN,
    tdsc.UNIDAD_NEGOCIO
FROM
    dw.dbo.TS_Fact_Recepcion tfr
LEFT JOIN dw.dbo.TS_Dim_Paciente tdp ON tdp.ID_PACIENTE = tfr.ID_PACIENTE
INNER JOIN dw.dbo.TS_Fact_Atencion tfa ON tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB
LEFT JOIN dw.dbo.TS_Dim_Servicio_Centro tdsc ON tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO
WHERE
    tfa.FECHA_HORA_FIN_ATE_MED BETWEEN DATEADD(DD, -1, CONVERT(DATE, GETDATE())) AND CONVERT(DATE, GETDATE())
    AND LEN(tdp.MAIL_1) >= 4
    AND tfr.ANULADA_RECEPCION = 'n'
    AND tfr.ANULADA_DET_RECEPCION = 'n'
    AND tfa.ESTADO_ATENCION = 'FINALIZADA'
    AND tfa.ANULADA_DET_ATENCION = 'n'
    AND tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
    AND tdsc.UNIDAD_NEGOCIO = 'Telefonico'
    AND tfr.ID_SERVICIO_CENTRO_FACTURA = 109
    AND tfr.ID_PACIENTE NOT IN (
        SELECT tfi.ID_PACIENTE
        FROM dw.dbo.TS_Fact_Internacion tfi
        LEFT JOIN dw.dbo.TS_Dim_Paciente tdp ON tdp.ID_PACIENTE = tfi.ID_PACIENTE
        WHERE tfi.FECHA_ALTA_MEDICA BETWEEN DATEADD(DD, -1, CONVERT(DATE, GETDATE())) AND CONVERT(DATE, GETDATE())
            AND LEN(tdp.MAIL_1) >= 4
            AND tfi.ANULADA_INTERNACION ='N'
            AND tfi.ID_ORIGEN_INTERNACION = 8
            AND tfi.ID_TIPO_ALTA <> 6
            AND tfi.ID_FUNCION_INTERNACION = 5
    )
    AND tfr.ID_PACIENTE NOT IN (
        SELECT id_paciente
        FROM dw.dbo.Dw_Registro_Encuestas s
        WHERE CONVERT(DATE, s.Fecha_Envio) = CONVERT(DATE, GETDATE())
    )
GROUP BY
    tfr.ID_PACIENTE,
    tdp.MAIL_1,
    DATEPART(w, FECHA_HORA_RECEP),
    tdsc.SERVICIO_ORIGEN,
    tdp.APELLIDO_NOMBRE_PACIENTE,
    tdsc.UNIDAD_NEGOCIO
""",
"Encuesta_AMU_Internos":"""SELECT
    tdp.MAIL_1 AS MAIL_PACIENTE,
    tfr.ID_PACIENTE,
    tdp.APELLIDO_NOMBRE_PACIENTE,
    tdsc.SERVICIO_ORIGEN,
    tdsc.UNIDAD_NEGOCIO
FROM
    dw.dbo.TS_Fact_Recepcion tfr
LEFT JOIN dw.dbo.TS_Dim_Paciente tdp ON tdp.ID_PACIENTE = tfr.ID_PACIENTE
INNER JOIN dw.dbo.TS_Fact_Atencion tfa ON tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB
LEFT JOIN dw.dbo.TS_Dim_Servicio_Centro tdsc ON tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO
WHERE
    tfa.FECHA_HORA_FIN_ATE_MED BETWEEN DATEADD(DD, -1, CONVERT(DATE, GETDATE())) AND CONVERT(DATE, GETDATE())
    AND LEN(tdp.MAIL_1) >= 4
    AND tfr.ANULADA_RECEPCION = 'n'
    AND tfr.ANULADA_DET_RECEPCION = 'n'
    AND tfa.ESTADO_ATENCION = 'FINALIZADA'
    AND tfa.ANULADA_DET_ATENCION = 'n'
    AND tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_INTERNADA'
    AND tdsc.UNIDAD_NEGOCIO = 'Atencion Medica de Urgencias'
    AND tfr.ID_SERVICIO_CENTRO_FACTURA = 109
    AND tfr.ID_PACIENTE IN (
        SELECT tfi.ID_PACIENTE
        FROM dw.dbo.TS_Fact_Internacion tfi
        LEFT JOIN dw.dbo.TS_Dim_Paciente tdp ON tdp.ID_PACIENTE = tfi.ID_PACIENTE
        WHERE tfi.FECHA_ALTA_MEDICA BETWEEN DATEADD(DD, -1, CONVERT(DATE, GETDATE())) AND CONVERT(DATE, GETDATE())
            AND LEN(tdp.MAIL_1) >= 4
            AND tfi.ANULADA_INTERNACION ='N'
            AND tfi.ID_ORIGEN_INTERNACION = 8
            AND tfi.ID_TIPO_ALTA <> 6
            AND tfi.ID_FUNCION_INTERNACION = 5
    )
    AND tfr.ID_PACIENTE NOT IN (
        SELECT id_paciente
        FROM dw.dbo.Dw_Registro_Encuestas s
        WHERE CONVERT(DATE, s.Fecha_Envio) = CONVERT(DATE, GETDATE())
    )
GROUP BY
    tfr.ID_PACIENTE,
    tdp.MAIL_1,
    DATEPART(w, FECHA_HORA_RECEP),
    tdsc.SERVICIO_ORIGEN,
    tdp.APELLIDO_NOMBRE_PACIENTE,
    tdsc.UNIDAD_NEGOCIO
"""
}


# Función para conectar a la base de datos con las credenciales cargadas desde el archivo JSON
def conectar_bd(configuracion):
    connection_string = f"DRIVER={{SQL Server}};SERVER={configuracion['server']};DATABASE={configuracion['database']};UID={configuracion['username']};PWD={configuracion['password']}"
    connection = pyodbc.connect(connection_string)
    return connection

# Función para obtener el diccionario de links
def obtener_diccionario_links(connection):
    cursor = connection.cursor()

    query_links = "SELECT * FROM DW.dbo.Dw_Links_Encuestas"
    cursor.execute(query_links)

    diccionario_links = {}
    for row in cursor.fetchall():
        diccionario_links[row.Encuesta] = row.Link
    return diccionario_links

# Función para enviar correos electrónicos
def enviar_correos(diccionario_links, diccionario_querys, conexion, configuracion):
    for unidad_negocio, query in diccionario_querys.items():
        lista_pacientes = []
        print(unidad_negocio)
        print(query)
        cursor = conexion.cursor()
        cursor.execute(query)
        resultados = pd.DataFrame(cursor.fetchall())
        
        for row in cursor.fetchall():
            print(row)
            # Añade cada paciente a la lista con sus detalles
            lista_pacientes.append([row.MAIL_PACIENTE, row.MAIL_PACIENTE, row.ID_PACIENTE, row.APELLIDO_NOMBRE_PACIENTE, row.SERVICIO_ORIGEN])

        
        # Obtén los resultados de la consulta en un DataFrame
        resultados = pd.DataFrame(cursor.fetchall())

         # Guarda el DataFrame en un archivo Excel
        nombre_excel = f"resultados_{unidad_negocio}.xlsx"
        resultados.to_excel(nombre_excel, index=False)


        #print(unidad_negocio)
        #print(lista_pacientes)
        
        # Verifica si hay un enlace asociado a la encuesta
        if unidad_negocio in diccionario_links:
            link_form = diccionario_links[unidad_negocio]
            enviar_correo_individual(lista_pacientes, link_form, unidad_negocio, configuracion)

# Función para enviar correo a un paciente individual
def enviar_correo_individual(lista_pacientes, link_form, unidad_negocio, configuracion):
    server = smtplib.SMTP(configuracion['email']['smtp_server'], configuracion['email']['smtp_port'])
    server.starttls()
    server.login(configuracion['email']['email_address'], configuracion['email']['email_password'])

    script_path = os.path.dirname(os.path.abspath(__file__))
    
# Definir rutas de imágenes una sola vez fuera del bucle
    rutaImagenHeader = os.path.join(script_path, "Header.png")
    rutaImagenFooter = os.path.join(script_path, "Footer.png")

    

    #for paciente in lista_pacientes:
    #    print(paciente[0])
        
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

    
    mensaje = f"""
                <html>
                    <style>
                        body {{
                            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                        }}
                    </style>
                    {HeaderHtml}
                    <body>
                        <br><br><center>¡Hola !<br><br>
                        ¿Nos ayudás a conocerte y brindarte un mejor servicio?<br>
                        Te invitamos a calificar tu experiencia en {unidad_negocio}<br><br>
                        {botonHtml}<br>
                        Te toma 1 minuto y los datos recabados son con fines estadísticos.<br><br>
                        ¡Gracias por tu tiempo!</center><br><br>
                        <br><br>
                        {FooterHtml}
                    </body>
                </html>
            """
    #fuente 11.25, agregar nombre, y cambiar unidad de negocio por negocio origen o lo que sea, pero que sea coloquial.
            
    msg = MIMEMultipart()
    msg.attach(MIMEText(mensaje, 'html'))
    
        # Adjuntar imágenes
    with open(rutaImagenHeader, 'rb') as file:
        imagen_adjunta = MIMEImage(file.read(), name=os.path.basename(rutaImagenHeader))
        imagen_adjunta.add_header('Content-ID', '<imagen>')
        msg.attach(imagen_adjunta)

    with open(rutaImagenFooter, 'rb') as file:
        imagen_firma_adjunta = MIMEImage(file.read(), name=os.path.basename(rutaImagenFooter))
        imagen_firma_adjunta.add_header('Content-ID', '<imagen2>')
        msg.attach(imagen_firma_adjunta)

    
    msg['Subject'] = 'Encuesta'
    msg['From'] = 'noreply.dataanalytics@sanjuandedios.org.ar'
    if env=="dev":
        msg['To'] = 'lfacundonazar@gmail.com'
    #else: 
    #    msg['To'] = paciente[0]
        pass
    print (msg['To'])
    #print(unidad_negocio, link_form)
    server.sendmail('noreply.dataanalytics@sanjuandedios.org.ar', msg['To'], msg.as_string())
    #insertar_registro_encuestas(conexion, paciente, unidad_negocio, env)
    print("mail enviado")
    server.quit()

# Función para insertar un registro en la tabla de encuestas después del envío
def insertar_registro_encuestas(conexion, paciente, unidad_negocio, env):
    cursor = conexion.cursor()
    fecha_envio = "GETDATE()"  

    query_insert = f"""
        INSERT INTO DW.dbo.Dw_Registro_Encuestas
        (ID_Paciente, Mail, Paciente, Servicio, Ambiente, Encuesta, Fecha_Envio)
        VALUES ({paciente[2]}, '{paciente[0]}', '{paciente[3]}', '{paciente[4]}', '{env}', '{unidad_negocio}', {fecha_envio});
    """

    cursor.execute(query_insert)
    conexion.commit()

try:
    # Cargar configuración de base de datos y correo
    configuracion = cargar_configuracion()
    print("config")
    conexion = conectar_bd(configuracion['database'])
    print("conexion")
    diccionario_links = obtener_diccionario_links(conexion)
    print("dicc_links")
    envio_correos = enviar_correos(diccionario_links, diccionario_querys, conexion, configuracion)
    print("Terminado")
except pyodbc.Error as e:
    print(f"Error de base de datos: {e}")
except smtplib.SMTPException as e:
    print(f"Error al enviar correos electrónicos: {e}")
finally:
    if 'conexion' in locals():
        conexion.close()