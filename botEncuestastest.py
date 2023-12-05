import pyodbc
import smtplib
from email.mime.text import MIMEText
import pandas as pd
import json
import os



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
    tdsc.SERVICIO_ESPECIALIDAD,
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
    tdsc.SERVICIO_ESPECIALIDAD,
    tdp.APELLIDO_NOMBRE_PACIENTE,
    tdsc.UNIDAD_NEGOCIO
""",
"Encuesta_AMU_Pediatricos":"""SELECT
    tdp.MAIL_1 AS MAIL_PACIENTE,
    tfr.ID_PACIENTE,
    tdp.APELLIDO_NOMBRE_PACIENTE,
    tdsc.SERVICIO_ESPECIALIDAD,
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
    tdsc.SERVICIO_ESPECIALIDAD,
    tdp.APELLIDO_NOMBRE_PACIENTE,
    tdsc.UNIDAD_NEGOCIO
""",
"Encuesta_AMU_Telefonicas":"""SELECT
    tdp.MAIL_1 AS MAIL_PACIENTE,
    tfr.ID_PACIENTE,
    tdp.APELLIDO_NOMBRE_PACIENTE,
    tdsc.SERVICIO_ESPECIALIDAD,
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
    tdsc.SERVICIO_ESPECIALIDAD,
    tdp.APELLIDO_NOMBRE_PACIENTE,
    tdsc.UNIDAD_NEGOCIO
""",
"Encuesta_AMU_Internos":"""SELECT
    tdp.MAIL_1 AS MAIL_PACIENTE,
    tfr.ID_PACIENTE,
    tdp.APELLIDO_NOMBRE_PACIENTE,
    tdsc.SERVICIO_ESPECIALIDAD,
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
    tdsc.SERVICIO_ESPECIALIDAD,
    tdp.APELLIDO_NOMBRE_PACIENTE,
    tdsc.UNIDAD_NEGOCIO
"""
}


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
        diccionario_links[row.Encuesta] = row.Link
    return diccionario_links

# Función para enviar correos electrónicos
def enviar_correos(diccionario_links, diccionario_querys, conexion, configuracion):
    for unidad_negocio, query in diccionario_querys.items():
        lista_pacientes = []
        cursor = conexion.cursor()
        cursor.execute(query)
        
        for row in cursor.fetchall():
            # Añade cada paciente a la lista con sus detalles
            lista_pacientes.append([row.MAIL_PACIENTE, row.MAIL_PACIENTE, row.ID_PACIENTE, row.APELLIDO_NOMBRE_PACIENTE, row.SERVICIO_ORIGEN])

        # Obtén los resultados de la consulta en un DataFrame
        resultados = pd.DataFrame(cursor.fetchall())

         # Guarda el DataFrame en un archivo Excel
        nombre_excel = f"resultados_{unidad_negocio}.xlsx"
        resultados.to_excel(nombre_excel, index=False)

        for row in cursor.fetchall():
            # Añade cada paciente a la lista con sus detalles
            lista_pacientes.append([row.MAIL_PACIENTE, row.MAIL_PACIENTE, row.ID_PACIENTE, row.APELLIDO_NOMBRE_PACIENTE, row.SERVICIO_ORIGEN])

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

    for paciente in lista_pacientes:
        print(paciente[0])
        msg = MIMEText(f"""
            <div class='row' style='text-align: justify'>Estimado paciente</div class='row'><br>
            <div class='row' style='text-align: justify'>Con el fin de conocer su experiencia respecto a nuestra atención, queremos realizarle algunas preguntas. La misma tomará 1 minuto y los datos recabados son con fines estadísticos.</div class='row'><br>
            <div class='row' style='text-align: justify'>Su colaboración nos ayudará a brindar un mejor servicio. A continuación le indicamos el link donde podrá realizar la <a href='{link_form}'>ENCUESTA</a></div class='row'><br>
            <div class='row' style='text-align: justify'>Desde ya, agradecemos su tiempo y predisposición.</div class='row'><br>
            <div class='row' style='text-align: justify'>Cordialmente.</div class='row'><div class='row' style='text-align: justify'><p style='font-weight:bold'>Servicio al Cliente</p></div class='row'><div class='row' style='text-align: justify'>Departamento de Calidad</div class='row'>
        """, 'html')
        msg['Subject'] = 'Encuesta'
        msg['From'] = 'noreply.dataanalytics@sanjuandedios.org.ar'
        if env=="dev":
            msg['To'] = 'lfacundonazar@gmail.com'
        else: 
            msg['To'] = paciente[0]
            pass
        print (msg['To'])
        #print(unidad_negocio, link_form)
        server.sendmail('noreply.dataanalytics@sanjuandedios.org.ar', 'lfacundonazar@gmail.com', msg.as_string())
        insertar_registro_encuestas(conexion, paciente, unidad_negocio, env)
        
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