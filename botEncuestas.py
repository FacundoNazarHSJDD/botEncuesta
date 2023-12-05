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
env=""

# Diccionario que mapea el nombre de la encuesta a la consulta SQL correspondiente
diccionario_querys={
                    "Encuesta_AMA_Castelar": """SELECT   tdp.MAIL_1 AS MAIL_PACIENTE
    , tfr.ID_PACIENTE 
    , tdp.APELLIDO_NOMBRE_PACIENTE 
    , tdsc.SERVICIO_ORIGEN
from dw.dbo.TS_Fact_Recepcion tfr 
left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfr.ID_PACIENTE 
inner join dw.dbo.TS_Fact_Atencion tfa on tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB 
left join dw.dbo.TS_Dim_Servicio_Centro tdsc on tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO 
where  tfa.FECHA_HORA_FIN_ATE_MED  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
and len (tdp.MAIL_1) >= 4
and tfr.ANULADA_RECEPCION = 'n'
and tfr.ANULADA_DET_RECEPCION = 'n'
and tfa.ESTADO_ATENCION = 'FINALIZADA'
and tfa.ANULADA_DET_ATENCION = 'n'
and tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
and tdsc.UNIDAD_NEGOCIO = 'Atencion Medica Ambulatoria'
and tfr.ID_CENTRO = 2
and tfr.ID_SERVICIO_CENTRO_REALIZA not in (4,144,153)
and tfr.COD_DET_PREST_RECEP_AMB  in (select   min(tfr.COD_DET_PREST_RECEP_AMB)
                        from dw.dbo.TS_Fact_Recepcion tfr 
                        left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfr.ID_PACIENTE 
                        inner join dw.dbo.TS_Fact_Atencion tfa on tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB 
                        left join dw.dbo.TS_Dim_Servicio_Centro tdsc on tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO 
                            where  tfa.FECHA_HORA_FIN_ATE_MED  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
                            and len (tdp.MAIL_1) >= 4
                            and tfr.ANULADA_RECEPCION = 'n'
                            and tfr.ANULADA_DET_RECEPCION = 'n'
                            and tfa.ESTADO_ATENCION = 'FINALIZADA'
                            and tfa.ANULADA_DET_ATENCION = 'n'
                            and tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
                            and tdsc.UNIDAD_NEGOCIO = 'Atencion Medica Ambulatoria'
                            and tfr.ID_CENTRO = 2 
                            and tfr.ID_SERVICIO_CENTRO_REALIZA not in (4,144,153)
                        group by  tfr.ID_PACIENTE  )
and tfr.ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))                    
group by tfr.ID_PACIENTE , tdp.MAIL_1 ,datepart (w,FECHA_HORA_RECEP)
    , tdp.APELLIDO_NOMBRE_PACIENTE     , tdsc.SERVICIO_ORIGEN
""",
"Encuesta_AMA_Ramos": """SELECT   tdp.MAIL_1 AS MAIL_PACIENTE
    , tfr.ID_PACIENTE 
    , tdp.APELLIDO_NOMBRE_PACIENTE 
    , tdsc.SERVICIO_ORIGEN
from dw.dbo.TS_Fact_Recepcion tfr 
left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfr.ID_PACIENTE 
inner join dw.dbo.TS_Fact_Atencion tfa on tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB 
left join dw.dbo.TS_Dim_Servicio_Centro tdsc on tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO 
where  tfa.FECHA_HORA_FIN_ATE_MED  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
and len (tdp.MAIL_1) = 4
and tfr.ANULADA_RECEPCION = 'n'
and tfr.ANULADA_DET_RECEPCION = 'n'
and tfa.ESTADO_ATENCION = 'FINALIZADA'
and tfa.ANULADA_DET_ATENCION = 'n'
and tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
and tdsc.UNIDAD_NEGOCIO = 'Atencion Medica Ambulatoria'
and tfr.ID_CENTRO = 1 --ramos
and tfr.ID_SERVICIO_CENTRO_REALIZA not in (4,144,153)
and tfr.COD_DET_PREST_RECEP_AMB  in (select   min(tfr.COD_DET_PREST_RECEP_AMB)
                        from dw.dbo.TS_Fact_Recepcion tfr 
                        left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfr.ID_PACIENTE 
                        inner join dw.dbo.TS_Fact_Atencion tfa on tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB 
                        left join dw.dbo.TS_Dim_Servicio_Centro tdsc on tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO 
                            where  tfa.FECHA_HORA_FIN_ATE_MED  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
                            and len (tdp.MAIL_1) = 4
                            and tfr.ANULADA_RECEPCION = 'n'
                            and tfr.ANULADA_DET_RECEPCION = 'n'
                            and tfa.ESTADO_ATENCION = 'FINALIZADA'
                            and tfa.ANULADA_DET_ATENCION = 'n'
                            and tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
                            and tdsc.UNIDAD_NEGOCIO = 'Atencion Medica Ambulatoria'
                            and tfr.ID_CENTRO = 1 --ramos
                            and tfr.ID_SERVICIO_CENTRO_REALIZA not in (4,144,153)
                        group by  tfr.ID_PACIENTE  )
and tfr.ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))                    
group by tfr.ID_PACIENTE , tdp.MAIL_1 ,datepart (w,FECHA_HORA_RECEP)
    , tdp.APELLIDO_NOMBRE_PACIENTE     , tdsc.SERVICIO_ORIGEN
""",
"Encuesta_AMU_Adultos":"""SELECT   tdp.MAIL_1 AS MAIL_PACIENTE
	, tfr.ID_PACIENTE 
	,tdp.APELLIDO_NOMBRE_PACIENTE 
	, tdsc.SERVICIO_ORIGEN , tdsc.UNIDAD_NEGOCIO 
 from dw.dbo.TS_Fact_Recepcion tfr 
left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfr.ID_PACIENTE 
inner join dw.dbo.TS_Fact_Atencion tfa on tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB 
left join dw.dbo.TS_Dim_Servicio_Centro tdsc on tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO 
where  tfa.FECHA_HORA_FIN_ATE_MED  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
and len (tdp.MAIL_1) >= 4
and tfr.ANULADA_RECEPCION = 'n'
and tfr.ANULADA_DET_RECEPCION = 'n'
and tfa.ESTADO_ATENCION = 'FINALIZADA'
and tfa.ANULADA_DET_ATENCION = 'n'
and tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
and tdsc.UNIDAD_NEGOCIO = 'Atencion Medica de Urgencias'
AND tfr.ID_SERVICIO_CENTRO_FACTURA = 109
and datediff(year,tdp.FECHA_NACIMIENTO_PACIENTE, getdate()) > 15
and tfr.ID_PACIENTE not in  (SELECT  tfi.ID_PACIENTE 
		 					from dw.dbo.TS_Fact_Internacion tfi 
							left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfi.ID_PACIENTE 
							where  tfi.FECHA_ALTA_MEDICA  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
							and len (tdp.MAIL_1) >= 4
							and tfi.ANULADA_INTERNACION ='N'
							and tfi.ID_ORIGEN_INTERNACION = 8
							and tfi.ID_TIPO_ALTA <> 6
							and tfi.ID_FUNCION_INTERNACION = 5)
and tfr.ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))        
group by tfr.ID_PACIENTE , tdp.MAIL_1 ,datepart (w,FECHA_HORA_RECEP)
	, tdsc.SERVICIO_ORIGEN,tdp.APELLIDO_NOMBRE_PACIENTE  , tdsc.UNIDAD_NEGOCIO
	
""",
"Encuesta_AMU_Pediatricos":"""SELECT   tdp.MAIL_1 AS MAIL_PACIENTE
	, tfr.ID_PACIENTE 
	,tdp.APELLIDO_NOMBRE_PACIENTE 
	, tdsc.SERVICIO_ORIGEN , tdsc.UNIDAD_NEGOCIO 
 from dw.dbo.TS_Fact_Recepcion tfr 
left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfr.ID_PACIENTE 
inner join dw.dbo.TS_Fact_Atencion tfa on tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB 
left join dw.dbo.TS_Dim_Servicio_Centro tdsc on tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO 
where  tfa.FECHA_HORA_FIN_ATE_MED  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
and len (tdp.MAIL_1) >= 4
and tfr.ANULADA_RECEPCION = 'n'
and tfr.ANULADA_DET_RECEPCION = 'n'
and tfa.ESTADO_ATENCION = 'FINALIZADA'
and tfa.ANULADA_DET_ATENCION = 'n'
and tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
and tdsc.UNIDAD_NEGOCIO = 'Atencion Medica de Urgencias'
AND tfr.ID_SERVICIO_CENTRO_FACTURA = 109
and datediff(year,tdp.FECHA_NACIMIENTO_PACIENTE, getdate()) < 15
and tfr.ID_PACIENTE not in  (SELECT  tfi.ID_PACIENTE 
		 					from dw.dbo.TS_Fact_Internacion tfi 
							left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfi.ID_PACIENTE 
							where  tfi.FECHA_ALTA_MEDICA  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
							and len (tdp.MAIL_1) >= 4
							and tfi.ANULADA_INTERNACION ='N'
							and tfi.ID_ORIGEN_INTERNACION = 8
							and tfi.ID_TIPO_ALTA <> 6
							and tfi.ID_FUNCION_INTERNACION = 5)
and tfr.ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))        
group by tfr.ID_PACIENTE , tdp.MAIL_1 ,datepart (w,FECHA_HORA_RECEP)	, tdsc.SERVICIO_ORIGEN,tdp.APELLIDO_NOMBRE_PACIENTE  , tdsc.UNIDAD_NEGOCIO
""",
"Encuesta_INT_QUIR_Adultos":"""SELECT MAIL_1 AS MAIL_PACIENTE, ID_PACIENTE, APELLIDO_NOMBRE_PACIENTE, SERVICIO AS SERVICIO_ORIGEN
FROM
(SELECT tfi.FECHA_INT, tfi.FECHA_ALTA_MEDICA, tdp.ID_PACIENTE, tdp.TIPO_NRO_DOC_PACIENTE, tdp.MAIL_1 , w.fecha_hora_ini_intervencion,tdsc.ID_SERVICIO_CENTRO, tdsc.SERVICIO,
tfi.ADULTO_PEDIATRICO, tdp.APELLIDO_NOMBRE_PACIENTE , case when fecha_hora_ini_intervencion > 0 then 'QUIRURGICO' else 'CLINICO' end as CLIN_QUIR
FROM DW.DBO.TS_Fact_Internacion tfi 
left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfi.ID_PACIENTE 
left join dw.dbo.TS_Dim_Servicio_Centro tdsc on tdsc.ID_SERVICIO_CENTRO = tfi.ID_SERVICIO_CENTRO 
left join (select ifaq.fecha_hora_ini_intervencion, idp.ID_PACIENTE, idp.TIPO_NRO_DOC_PERSONAL  from indra.dbo.IND_Fact_Atencion_Quirofano ifaq
left join indra.dbo.IND_Dim_Paciente idp on idp.ID_PACIENTE = ifaq.id_paciente) w
on tdp.tipo_nro_doc_paciente = w.TIPO_NRO_DOC_PERSONAL and w.FECHA_HORA_INI_INTERVENCION between cast(tfi.FECHA_INT as date) and cast((dateadd(day,1,tfi.FECHA_ALTA_ADMIN)) as date)
where tfi.FECHA_ALTA_MEDICA between cast(getdate()-1 as date) and getdate()
and tfi.ID_TIPO_ALTA <> 6
and tfi.REGISTRO_ACTIVO = 'S'
and tfi.ANULADA_INTERNACION = 'N'
and len(tdp.MAIL_1) >= 4
and ID_SECTOR_ADMISION <> 1
and tfi.ID_SERVICIO_CENTRO not in (140,59,86,192,18,43,151,193)
and tfi.ID_FUNCION_INTERNACION <> 5) t1
where t1.CLIN_QUIR = 'QUIRURGICO' and t1.ADULTO_PEDIATRICO = 'ADULTO'
and ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))        
group by MAIL_1, ID_PACIENTE, APELLIDO_NOMBRE_PACIENTE, SERVICIO
""",
"Encuesta_INT_QUIR_Pediatricos":"""SELECT MAIL_1 AS MAIL_PACIENTE, ID_PACIENTE, APELLIDO_NOMBRE_PACIENTE, SERVICIO AS SERVICIO_ORIGEN
FROM
(SELECT tfi.FECHA_INT, tfi.FECHA_ALTA_MEDICA, tdp.ID_PACIENTE, tdp.TIPO_NRO_DOC_PACIENTE, tdp.MAIL_1 , w.fecha_hora_ini_intervencion,tdsc.ID_SERVICIO_CENTRO, tdsc.SERVICIO,
tfi.ADULTO_PEDIATRICO, tdp.APELLIDO_NOMBRE_PACIENTE , case when fecha_hora_ini_intervencion > 0 then 'QUIRURGICO' else 'CLINICO' end as CLIN_QUIR
FROM DW.DBO.TS_Fact_Internacion tfi 
left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfi.ID_PACIENTE 
left join dw.dbo.TS_Dim_Servicio_Centro tdsc on tdsc.ID_SERVICIO_CENTRO = tfi.ID_SERVICIO_CENTRO 
left join (select ifaq.fecha_hora_ini_intervencion, idp.ID_PACIENTE, idp.TIPO_NRO_DOC_PERSONAL  from indra.dbo.IND_Fact_Atencion_Quirofano ifaq
left join indra.dbo.IND_Dim_Paciente idp on idp.ID_PACIENTE = ifaq.id_paciente) w
on tdp.tipo_nro_doc_paciente = w.TIPO_NRO_DOC_PERSONAL and w.FECHA_HORA_INI_INTERVENCION between cast(tfi.FECHA_INT as date) and cast((dateadd(day,1,tfi.FECHA_ALTA_ADMIN)) as date)
where tfi.FECHA_ALTA_MEDICA between cast(getdate()-1 as date) and getdate()
and tfi.ID_TIPO_ALTA <> 6
and tfi.REGISTRO_ACTIVO = 'S'
and tfi.ANULADA_INTERNACION = 'N'
and len(tdp.MAIL_1) >= 4
and ID_SECTOR_ADMISION <> 1
and tfi.ID_SERVICIO_CENTRO not in (140,59,86,192,18,43,151,193)
and tfi.ID_FUNCION_INTERNACION <> 5) t1
where t1.CLIN_QUIR = 'QUIRURGICO' and t1.ADULTO_PEDIATRICO = 'PEDIATRICO'
and ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate())) 
group by MAIL_1, ID_PACIENTE, APELLIDO_NOMBRE_PACIENTE, SERVICIO
""",
"Encuesta_INT_CLIN_Adultos":"""SELECT MAIL_1 AS MAIL_PACIENTE, ID_PACIENTE, APELLIDO_NOMBRE_PACIENTE, SERVICIO AS SERVICIO_ORIGEN
FROM
(SELECT tfi.FECHA_INT, tfi.FECHA_ALTA_MEDICA, tdp.ID_PACIENTE, tdp.TIPO_NRO_DOC_PACIENTE, tdp.MAIL_1 , w.fecha_hora_ini_intervencion,tdsc.ID_SERVICIO_CENTRO, tdsc.SERVICIO,
tfi.ADULTO_PEDIATRICO, tdp.APELLIDO_NOMBRE_PACIENTE , case when fecha_hora_ini_intervencion > 0 then 'QUIRURGICO' else 'CLINICO' end as CLIN_QUIR
FROM DW.DBO.TS_Fact_Internacion tfi 
left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfi.ID_PACIENTE 
left join dw.dbo.TS_Dim_Servicio_Centro tdsc on tdsc.ID_SERVICIO_CENTRO = tfi.ID_SERVICIO_CENTRO 
left join (select ifaq.fecha_hora_ini_intervencion, idp.ID_PACIENTE, idp.TIPO_NRO_DOC_PERSONAL  from indra.dbo.IND_Fact_Atencion_Quirofano ifaq
left join indra.dbo.IND_Dim_Paciente idp on idp.ID_PACIENTE = ifaq.id_paciente) w
on tdp.tipo_nro_doc_paciente = w.TIPO_NRO_DOC_PERSONAL and w.FECHA_HORA_INI_INTERVENCION between cast(tfi.FECHA_INT as date) and cast((dateadd(day,1,tfi.FECHA_ALTA_ADMIN)) as date)
where tfi.FECHA_ALTA_MEDICA between cast(getdate()-1 as date) and getdate()
and tfi.ID_TIPO_ALTA <> 6
and tfi.REGISTRO_ACTIVO = 'S'
and tfi.ANULADA_INTERNACION = 'N'
and len(tdp.MAIL_1) >= 4
and ID_SECTOR_ADMISION <> 1
and tfi.ID_SERVICIO_CENTRO not in (140,59,86,192,18,43,151,193)
and tfi.ID_FUNCION_INTERNACION <> 5) t1
where t1.CLIN_QUIR = 'CLINICO' and t1.ADULTO_PEDIATRICO = 'ADULTO'
and ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))        
group by MAIL_1, ID_PACIENTE, APELLIDO_NOMBRE_PACIENTE, SERVICIO
""",
"Encuesta_INT_CLIN_Pediatricos":"""SELECT MAIL_1 AS MAIL_PACIENTE, ID_PACIENTE, APELLIDO_NOMBRE_PACIENTE, SERVICIO AS SERVICIO_ORIGEN
FROM
(SELECT tfi.FECHA_INT, tfi.FECHA_ALTA_MEDICA, tdp.ID_PACIENTE, tdp.TIPO_NRO_DOC_PACIENTE, tdp.MAIL_1 , w.fecha_hora_ini_intervencion,tdsc.ID_SERVICIO_CENTRO, tdsc.SERVICIO,
tfi.ADULTO_PEDIATRICO, tdp.APELLIDO_NOMBRE_PACIENTE , case when fecha_hora_ini_intervencion > 0 then 'QUIRURGICO' else 'CLINICO' end as CLIN_QUIR
FROM DW.DBO.TS_Fact_Internacion tfi 
left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfi.ID_PACIENTE 
left join dw.dbo.TS_Dim_Servicio_Centro tdsc on tdsc.ID_SERVICIO_CENTRO = tfi.ID_SERVICIO_CENTRO 
left join (select ifaq.fecha_hora_ini_intervencion, idp.ID_PACIENTE, idp.TIPO_NRO_DOC_PERSONAL  from indra.dbo.IND_Fact_Atencion_Quirofano ifaq
left join indra.dbo.IND_Dim_Paciente idp on idp.ID_PACIENTE = ifaq.id_paciente) w
on tdp.tipo_nro_doc_paciente = w.TIPO_NRO_DOC_PERSONAL and w.FECHA_HORA_INI_INTERVENCION between cast(tfi.FECHA_INT as date) and cast((dateadd(day,1,tfi.FECHA_ALTA_ADMIN)) as date)
where tfi.FECHA_ALTA_MEDICA between cast(getdate()-1 as date) and getdate()
and tfi.ID_TIPO_ALTA <> 6
and tfi.REGISTRO_ACTIVO = 'S'
and tfi.ANULADA_INTERNACION = 'N'
and len(tdp.MAIL_1) >= 4
and ID_SECTOR_ADMISION <> 1
and tfi.ID_SERVICIO_CENTRO not in (140,59,86,192,18,43,151,193)
and tfi.ID_FUNCION_INTERNACION <> 5) t1
where t1.CLIN_QUIR = 'CLINICO' and t1.ADULTO_PEDIATRICO = 'ADULTO'
and ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))        
group by MAIL_1, ID_PACIENTE, APELLIDO_NOMBRE_PACIENTE, SERVICIO
""",
"Encuesta_DI_Castelar":"""SELECT   tdp.MAIL_1 AS MAIL_PACIENTE
	    , tfr.ID_PACIENTE 
	    , tdp.APELLIDO_NOMBRE_PACIENTE 
	    , tdsc.SERVICIO_ORIGEN  
	 from dw.dbo.TS_Fact_Recepcion tfr 
	left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfr.ID_PACIENTE 
	inner join dw.dbo.TS_Fact_Atencion tfa on tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB 
	left join dw.dbo.TS_Dim_Servicio_Centro tdsc on tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO 
	where  tfa.FECHA_HORA_FIN_ATE_MED  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
	and len (tdp.MAIL_1) >= 4
	and tfr.ANULADA_RECEPCION = 'n'
	and tfr.ANULADA_DET_RECEPCION = 'n'
	and tfa.ESTADO_ATENCION = 'FINALIZADA'
	and tfa.ANULADA_DET_ATENCION = 'n'
	and tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
	and tdsc.UNIDAD_NEGOCIO = 'Diagnostico por Imagenes'
	AND tfr.ID_SERVICIO_CENTRO_FACTURA <> 109
	and tfr.ID_CENTRO = 2
	group by tfr.ID_PACIENTE , tdp.MAIL_1 ,datepart (w,FECHA_HORA_RECEP)
	    , tdp.APELLIDO_NOMBRE_PACIENTE , tdsc.SERVICIO_ORIGEN 
""",
"Encuesta_DI_Ramos":"""SELECT   tdp.MAIL_1 AS MAIL_PACIENTE
	    , tfr.ID_PACIENTE 
	    , tdp.APELLIDO_NOMBRE_PACIENTE 
	    , tdsc.SERVICIO_ORIGEN  
	 from dw.dbo.TS_Fact_Recepcion tfr 
	left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfr.ID_PACIENTE 
	inner join dw.dbo.TS_Fact_Atencion tfa on tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB 
	left join dw.dbo.TS_Dim_Servicio_Centro tdsc on tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO 
	where  tfa.FECHA_HORA_FIN_ATE_MED  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
	and len (tdp.MAIL_1) >= 4
	and tfr.ANULADA_RECEPCION = 'n'
	and tfr.ANULADA_DET_RECEPCION = 'n'
	and tfa.ESTADO_ATENCION = 'FINALIZADA'
	and tfa.ANULADA_DET_ATENCION = 'n'
	and tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
	and tdsc.UNIDAD_NEGOCIO = 'Diagnostico por Imagenes'
	AND tfr.ID_SERVICIO_CENTRO_FACTURA <> 109
	and tfr.ID_CENTRO = 1 --ramos
	--and tfr.ID_CENTRO = 2 --castelar
and tfr.ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))        
	group by tfr.ID_PACIENTE , tdp.MAIL_1 ,datepart (w,FECHA_HORA_RECEP)
	    , tdp.APELLIDO_NOMBRE_PACIENTE , tdsc.SERVICIO_ORIGEN  
""",
"Encuesta_LAB_Castelar":"""SELECT   tdp.MAIL_1 AS MAIL_PACIENTE
	    , tfr.ID_PACIENTE 
	    , tdp.APELLIDO_NOMBRE_PACIENTE 
	    , tdsc.SERVICIO_ORIGEN  
	 from dw.dbo.TS_Fact_Recepcion tfr 
	left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfr.ID_PACIENTE 
	inner join dw.dbo.TS_Fact_Atencion tfa on tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB 
	left join dw.dbo.TS_Dim_Servicio_Centro tdsc on tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO 
	where  tfa.FECHA_HORA_FIN_ATE_MED  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
	and len (tdp.MAIL_1) >= 4
	and tfr.ANULADA_RECEPCION = 'n'
	and tfr.ANULADA_DET_RECEPCION = 'n'
	and tfa.ESTADO_ATENCION = 'FINALIZADA'
	and tfa.ANULADA_DET_ATENCION = 'n'
	and tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
	and tdsc.UNIDAD_NEGOCIO = 'Laboratorio Clinico'
	AND tfr.ID_SERVICIO_CENTRO_FACTURA <> 109
	and tfr.ID_CENTRO = 2
and tfr.ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))        
	group by tfr.ID_PACIENTE , tdp.MAIL_1 ,datepart (w,FECHA_HORA_RECEP)
	    , tdp.APELLIDO_NOMBRE_PACIENTE     , tdsc.SERVICIO_ORIGEN  """,
"Encuesta_LAB_Ramos":"""SELECT   tdp.MAIL_1 AS MAIL_PACIENTE
	    , tfr.ID_PACIENTE 
	    , tdp.APELLIDO_NOMBRE_PACIENTE 
	    , tdsc.SERVICIO_ORIGEN  
	 from dw.dbo.TS_Fact_Recepcion tfr 
	left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfr.ID_PACIENTE 
	inner join dw.dbo.TS_Fact_Atencion tfa on tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB 
	left join dw.dbo.TS_Dim_Servicio_Centro tdsc on tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO 
	where  tfa.FECHA_HORA_FIN_ATE_MED  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
	and len (tdp.MAIL_1) >= 4
	and tfr.ANULADA_RECEPCION = 'n'
	and tfr.ANULADA_DET_RECEPCION = 'n'
	and tfa.ESTADO_ATENCION = 'FINALIZADA'
	and tfa.ANULADA_DET_ATENCION = 'n'
	and tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
	and tdsc.UNIDAD_NEGOCIO = 'Laboratorio Clinico'
	AND tfr.ID_SERVICIO_CENTRO_FACTURA <> 109
	and tfr.ID_CENTRO = 1 --ramos
and tfr.ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))        
	group by tfr.ID_PACIENTE , tdp.MAIL_1 ,datepart (w,FECHA_HORA_RECEP)
	    , tdp.APELLIDO_NOMBRE_PACIENTE     , tdsc.SERVICIO_ORIGEN """,
"Encuesta_OBS_Adultos":"""SELECT   tdp.MAIL_1 AS MAIL_PACIENTE
		, tfi.ID_PACIENTE 
		, tdp.APELLIDO_NOMBRE_PACIENTE 
		,tdsc.SERVICIO_ORIGEN 
	 from dw.dbo.TS_Fact_Internacion tfi 
	left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfi.ID_PACIENTE 
	left join dw.dbo.TS_Dim_Servicio_Centro tdsc on tdsc.ID_SERVICIO_CENTRO = tfi.ID_SERVICIO_CENTRO  
	where  tfi.FECHA_ALTA_MEDICA  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
	and len (tdp.MAIL_1) >= 4
	and tfi.ANULADA_INTERNACION ='N'
	and tfi.ID_ORIGEN_INTERNACION = 8
	and tfi.ID_TIPO_ALTA <> 6
	and tfi.ID_FUNCION_INTERNACION = 5
	and tfi.ADULTO_PEDIATRICO like 'PEDIATRICO'
and tfi.ID_PACIENTE not in (select id_paciente from dw.dbo.TS_Fact_Internacion tfi2 where ID_FUNCION_INTERNACIon <> 5 and convert(nvarchar(7),tfi2.FECHA_INT,120) in (convert(nvarchar(7),tfi.FECHA_INT,120)))
and tfi.ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))        
	group by tfi.ID_PACIENTE,tdp.APELLIDO_NOMBRE_PACIENTE ,tdsc.SERVICIO_ORIGEN , tdp.MAIL_1, datepart (w,tfi.FECHA_ALTA_MEDICA ) """,
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
        #print(query)
        cursor = conexion.cursor()
        cursor.execute(query)
        
        for row in cursor.fetchall():
            #print(row)
            # Añade cada paciente a la lista con sus detalles
            lista_pacientes.append([row.MAIL_PACIENTE, row.ID_PACIENTE, row.APELLIDO_NOMBRE_PACIENTE, row.SERVICIO_ORIGEN])

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

    

    for paciente in lista_pacientes:
        #print(paciente[0])
        
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
                                font-size: 11.25px;
                            }}
                        </style>
                        {HeaderHtml}
                        <body>
                            <br><br><center>¡Hola {obtener_iniciales_mayusculas_y_minusculas(paciente[2])}!<br><br>
                            ¿Nos ayudás a conocerte y brindarte un mejor servicio?<br>
                            Te invitamos a calificar tu experiencia en {obtener_iniciales_mayusculas_y_minusculas(paciente[3])}<br><br>
                            {botonHtml}<br>
                            Te toma 1 minuto y los datos recabados son con fines estadísticos.<br><br>
                            ¡Gracias por tu tiempo!</center><br><br>
                            <br><br>
                            {FooterHtml}
                        </body>
                    </html>
                """
                
        msg = MIMEMultipart()
        msg.attach(MIMEText(mensaje, 'html'))
        
            # Adjuntar imágenes
        with open(rutaImagenHeader, 'rb') as file:
            imagen_adjunta = MIMEImage(file.read(), name=os.path.basename(rutaImagenHeader))
            imagen_adjunta.add_header('Content-ID', '<imagen>')
            msg.attach(imagen_adjunta)

        with open(rutaImagenFooter, 'rb') as file:
            imagen_firma_adjunta = MIMEImage(file.read(), name=os.path.basename(rutaImagenFooter))
            imagen_firma_adjunta.add_header('Content-ID','<imagen2>')
            msg.attach(imagen_firma_adjunta)

        msg['Subject'] = 'Encuesta'
        msg['From'] = configuracion['email']['email_address']
        if env=="dev":
            msg['To'] = 'lfacundonazar@gmail.com'
        else: 
            msg['To'] = paciente[0]
            pass
        print (msg['To'])
        #print(unidad_negocio, link_form)
        server.sendmail(configuracion['email']['email_address'], msg['To'], msg.as_string())
        insertar_registro_encuestas(conexion, paciente, unidad_negocio, env)
        print("mail enviado")
    server.quit()

# Función para insertar un registro en la tabla de encuestas después del envío
def insertar_registro_encuestas(conexion, paciente, unidad_negocio, env):
    cursor = conexion.cursor()
    fecha_envio = "GETDATE()"  

    query_insert = f"""
        INSERT INTO DW.dbo.Dw_Registro_Encuestas
        (ID_Paciente, Mail, Paciente, Servicio, Ambiente, Encuesta, Fecha_Envio)
        VALUES ({paciente[1]}, '{paciente[0]}', '{paciente[2]}', '{paciente[3]}', '{env}', '{unidad_negocio}', {fecha_envio});
    """

    cursor.execute(query_insert)
    conexion.commit()

def obtener_iniciales_mayusculas_y_minusculas(cadena):
    palabras = cadena.split()
    iniciales = [palabra[:1].upper() + palabra[1:].lower() for palabra in palabras]
    return ' '.join(iniciales)

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