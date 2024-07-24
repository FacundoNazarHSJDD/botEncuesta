import sys
import cConfiguracion
import cBaseDatos
import cEncuesta

env = "PRD"
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
and tdsc.UNIDAD_NEGOCIO IN ('Atencion Medica Ambulatoria','Polisomnografia','Endoscopia')
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
                            and tdsc.UNIDAD_NEGOCIO IN ('Atencion Medica Ambulatoria','Polisomnografia','Endoscopia')
                            and tfr.ID_CENTRO = 2 
                            and tfr.ID_SERVICIO_CENTRO_REALIZA not in (4,144,153)
                        group by  tfr.ID_PACIENTE  )
and tfr.ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))                    
group by tfr.ID_PACIENTE , tdp.MAIL_1 ,datepart (w,FECHA_HORA_RECEP)
    , tdp.APELLIDO_NOMBRE_PACIENTE     , tdsc.SERVICIO_ORIGEN
order by SERVICIO_ORIGEN
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
and len (tdp.MAIL_1) > 4
and tfr.ANULADA_RECEPCION = 'n'
and tfr.ANULADA_DET_RECEPCION = 'n'
and tfa.ESTADO_ATENCION = 'FINALIZADA'
and tfa.ANULADA_DET_ATENCION = 'n'
and tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
and tdsc.UNIDAD_NEGOCIO IN ('Atencion Medica Ambulatoria','Polisomnografia','Endoscopia')
and tfr.ID_CENTRO = 1 --ramos
and tfr.ID_SERVICIO_CENTRO_REALIZA not in (4,144,153)
and tfr.COD_DET_PREST_RECEP_AMB  in (select   min(tfr.COD_DET_PREST_RECEP_AMB)
                        from dw.dbo.TS_Fact_Recepcion tfr 
                        left join dw.dbo.TS_Dim_Paciente tdp on tdp.ID_PACIENTE = tfr.ID_PACIENTE 
                        inner join dw.dbo.TS_Fact_Atencion tfa on tfa.COD_DET_PREST_RECEP_AMB = tfr.COD_DET_PREST_RECEP_AMB 
                        left join dw.dbo.TS_Dim_Servicio_Centro tdsc on tfa.ID_SERVICIO_CENTRO = tdsc.ID_SERVICIO_CENTRO 
                            where  tfa.FECHA_HORA_FIN_ATE_MED  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
                            and len (tdp.MAIL_1) > 4
                            and tfr.ANULADA_RECEPCION = 'n'
                            and tfr.ANULADA_DET_RECEPCION = 'n'
                            and tfa.ESTADO_ATENCION = 'FINALIZADA'
                            and tfa.ANULADA_DET_ATENCION = 'n'
                            and tfa.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
                            and tdsc.UNIDAD_NEGOCIO IN ('Atencion Medica Ambulatoria','Polisomnografia','Endoscopia')
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
and datediff(year,tdp.FECHA_NACIMIENTO_PACIENTE, getdate()) <= 15
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
"Encuesta_INT_QUIR_Adultos":"""SELECT   
	paciente.MAIL_1 AS MAIL_PACIENTE
    , recep.ID_PACIENTE	AS ID_PACIENTE
    , paciente.APELLIDO_NOMBRE_PACIENTE
    , serv_cen.SERVICIO_ORIGEN

	 from dw.dbo.TS_Fact_Recepcion recep 
	--TRAE ID PACIENTE DE DIM PACIENTE
	left join dw.dbo.TS_Dim_Paciente paciente on paciente.ID_PACIENTE = recep.ID_PACIENTE 
	--TRAE LA INFO DE LA ATENCION DE LA FACT ATENCION
	inner join dw.dbo.TS_Fact_Atencion atencion on atencion.COD_DET_PREST_RECEP_AMB = recep.COD_DET_PREST_RECEP_AMB 
	--TRAE LA DESCRIPCION DEL SERVICIO DE LA DIM SERVICIO CENTRO
	left join dw.dbo.TS_Dim_Servicio_Centro serv_cen on atencion.ID_SERVICIO_CENTRO = serv_cen.ID_SERVICIO_CENTRO 
	
where  atencion.FECHA_HORA_FIN_ATE_MED  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
	and len (paciente.MAIL_1) >= 4
	and recep.ANULADA_RECEPCION = 'n'
	and recep.ANULADA_DET_RECEPCION = 'n'
	and atencion.ESTADO_ATENCION = 'FINALIZADA'
	and atencion.ANULADA_DET_ATENCION = 'n'
	and atencion.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
	and serv_cen.UNIDAD_NEGOCIO = 'Diagnostico por Imagenes'
	and recep.ID_SERVICIO_CENTRO_FACTURA <> 109
	and recep.ID_CENTRO = 2
	and recep.COD_DET_PREST_RECEP_AMB  in (select   
											min(recep.COD_DET_PREST_RECEP_AMB)
										from dw.dbo.TS_Fact_Recepcion recep 
											left join dw.dbo.TS_Dim_Paciente paciente on paciente.ID_PACIENTE = recep.ID_PACIENTE 
											inner join dw.dbo.TS_Fact_Atencion atencion on atencion.COD_DET_PREST_RECEP_AMB = recep.COD_DET_PREST_RECEP_AMB 
											left join dw.dbo.TS_Dim_Servicio_Centro serv_cen on atencion.ID_SERVICIO_CENTRO = serv_cen.ID_SERVICIO_CENTRO 
										where  atencion.FECHA_HORA_FIN_ATE_MED  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
											and len (paciente.MAIL_1) >= 4
											and recep.ANULADA_RECEPCION = 'n'
											and recep.ANULADA_DET_RECEPCION = 'n'
											and atencion.ESTADO_ATENCION = 'FINALIZADA'
											and atencion.ANULADA_DET_ATENCION = 'n'
											and atencion.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
											and serv_cen.UNIDAD_NEGOCIO = 'Diagnostico por Imagenes'
											and recep.ID_CENTRO = 2 
											--and recep.ID_SERVICIO_CENTRO_REALIZA not in (4,144,153)
										group by  recep.ID_PACIENTE  )
	and recep.ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))
group by 
	recep.ID_PACIENTE ,
	paciente.MAIL_1 ,
	datepart (w,FECHA_HORA_RECEP) ,
	paciente.APELLIDO_NOMBRE_PACIENTE , 
	serv_cen.SERVICIO_ORIGEN ,
	recep.COD_DET_PREST_RECEP_AMB
""",
"Encuesta_INT_QUIR_Pediatricos":"""SELECT
	MAIL_1 AS MAIL_PACIENTE, 
	ID_PACIENTE, 
	APELLIDO_NOMBRE_PACIENTE, 
	SERVICIO AS SERVICIO_ORIGEN
FROM
	(SELECT 
		tfi.FECHA_INT, 
		tfi.FECHA_ALTA_MEDICA, 
		tdp.ID_PACIENTE, 
		tdp.TIPO_NRO_DOC_PACIENTE, 
		tdp.MAIL_1 , 
		w.fecha_hora_ini_intervencion,tdsc.ID_SERVICIO_CENTRO, 
		tdsc.SERVICIO,
		tfi.ADULTO_PEDIATRICO, 
		tdp.APELLIDO_NOMBRE_PACIENTE , 
		case when fecha_hora_ini_intervencion > 0 then 'QUIRURGICO' else 'CLINICO' end as CLIN_QUIR
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
group by 
	MAIL_1,
	ID_PACIENTE,
	APELLIDO_NOMBRE_PACIENTE,
	SERVICIO
""",
"Encuesta_INT_CLIN_Adultos":"""SELECT 
	MAIL_1 AS MAIL_PACIENTE,
	ID_PACIENTE,
	APELLIDO_NOMBRE_PACIENTE,
	SERVICIO AS SERVICIO_ORIGEN
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
where t1.CLIN_QUIR = 'CLINICO' and t1.ADULTO_PEDIATRICO = 'PEDIATRICO'
and ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))        
group by MAIL_1, ID_PACIENTE, APELLIDO_NOMBRE_PACIENTE, SERVICIO
""",
"Encuesta_DI_Castelar":"""SELECT   
	paciente.MAIL_1 AS MAIL_PACIENTE
    , recep.ID_PACIENTE	AS ID_PACIENTE
    , paciente.APELLIDO_NOMBRE_PACIENTE AS NOMBRE_PACIENTE
    , serv_cen.SERVICIO_ORIGEN AS SERVICIO_ORIGEN

	 from dw.dbo.TS_Fact_Recepcion recep 
	--TRAE ID PACIENTE DE DIM PACIENTE
	left join dw.dbo.TS_Dim_Paciente paciente on paciente.ID_PACIENTE = recep.ID_PACIENTE 
	--TRAE LA INFO DE LA ATENCION DE LA FACT ATENCION
	inner join dw.dbo.TS_Fact_Atencion atencion on atencion.COD_DET_PREST_RECEP_AMB = recep.COD_DET_PREST_RECEP_AMB 
	--TRAE LA DESCRIPCION DEL SERVICIO DE LA DIM SERVICIO CENTRO
	left join dw.dbo.TS_Dim_Servicio_Centro serv_cen on atencion.ID_SERVICIO_CENTRO = serv_cen.ID_SERVICIO_CENTRO 
	
where  atencion.FECHA_HORA_FIN_ATE_MED  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
	and len (paciente.MAIL_1) >= 4
	and recep.ANULADA_RECEPCION = 'n'
	and recep.ANULADA_DET_RECEPCION = 'n'
	and atencion.ESTADO_ATENCION = 'FINALIZADA'
	and atencion.ANULADA_DET_ATENCION = 'n'
	and atencion.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
	and serv_cen.UNIDAD_NEGOCIO = 'Diagnostico por Imagenes'
	and recep.ID_SERVICIO_CENTRO_FACTURA <> 109
	and recep.ID_CENTRO = 2
	and recep.COD_DET_PREST_RECEP_AMB  in (select   
											min(recep.COD_DET_PREST_RECEP_AMB)
										from dw.dbo.TS_Fact_Recepcion recep 
											left join dw.dbo.TS_Dim_Paciente paciente on paciente.ID_PACIENTE = recep.ID_PACIENTE 
											inner join dw.dbo.TS_Fact_Atencion atencion on atencion.COD_DET_PREST_RECEP_AMB = recep.COD_DET_PREST_RECEP_AMB 
											left join dw.dbo.TS_Dim_Servicio_Centro serv_cen on atencion.ID_SERVICIO_CENTRO = serv_cen.ID_SERVICIO_CENTRO 
										where  atencion.FECHA_HORA_FIN_ATE_MED  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
											and len (paciente.MAIL_1) >= 4
											and recep.ANULADA_RECEPCION = 'n'
											and recep.ANULADA_DET_RECEPCION = 'n'
											and atencion.ESTADO_ATENCION = 'FINALIZADA'
											and atencion.ANULADA_DET_ATENCION = 'n'
											and atencion.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
											and serv_cen.UNIDAD_NEGOCIO = 'Diagnostico por Imagenes'
											and recep.ID_CENTRO = 2 
											--and recep.ID_SERVICIO_CENTRO_REALIZA not in (4,144,153)
										group by  recep.ID_PACIENTE  )
	and recep.ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))
group by 
	recep.ID_PACIENTE ,
	paciente.MAIL_1 ,
	datepart (w,FECHA_HORA_RECEP) ,
	paciente.APELLIDO_NOMBRE_PACIENTE , 
	serv_cen.SERVICIO_ORIGEN ,
	recep.COD_DET_PREST_RECEP_AMB
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
	and tfa.COD_DET_PREST_RECEP_AMB  in (select   
											min(recep.COD_DET_PREST_RECEP_AMB)
										from dw.dbo.TS_Fact_Recepcion recep 
											left join dw.dbo.TS_Dim_Paciente paciente on paciente.ID_PACIENTE = recep.ID_PACIENTE 
											inner join dw.dbo.TS_Fact_Atencion atencion on atencion.COD_DET_PREST_RECEP_AMB = recep.COD_DET_PREST_RECEP_AMB 
											left join dw.dbo.TS_Dim_Servicio_Centro serv_cen on atencion.ID_SERVICIO_CENTRO = serv_cen.ID_SERVICIO_CENTRO 
										where  atencion.FECHA_HORA_FIN_ATE_MED  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
											and len (paciente.MAIL_1) >= 4
											and recep.ANULADA_RECEPCION = 'n'
											and recep.ANULADA_DET_RECEPCION = 'n'
											and atencion.ESTADO_ATENCION = 'FINALIZADA'
											and atencion.ANULADA_DET_ATENCION = 'n'
											and atencion.FLAG_AMBULATORIO_INTERNADO = 'ATENCION_AMBULATORIA'
											and serv_cen.UNIDAD_NEGOCIO = 'Diagnostico por Imagenes'
											and recep.ID_CENTRO = 1 
											and recep.ID_SERVICIO_CENTRO_REALIZA not in (4,144,153)
										group by  recep.ID_PACIENTE  )
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
	    , tdp.APELLIDO_NOMBRE_PACIENTE     , tdsc.SERVICIO_ORIGEN    """,
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
	where tfi.FECHA_ALTA_MEDICA  BETWEEN dateadd(DD,-1,CONVERT(DATE,GETDATE())) AND CONVERT(DATE,GETDATE())
	and len (tdp.MAIL_1) >= 4
	and tfi.ANULADA_INTERNACION ='N'
	and tfi.ID_ORIGEN_INTERNACION = 8
	and tfi.ID_TIPO_ALTA <> 6
	and tfi.ID_FUNCION_INTERNACION = 5
and tfi.ID_PACIENTE not in (select id_paciente from dw.dbo.TS_Fact_Internacion tfi2 where ID_FUNCION_INTERNACION <> 5 and convert(nvarchar(7),tfi2.FECHA_INT,120) in (convert(nvarchar(7),tfi.FECHA_INT,120)))
and tfi.ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))        
	group by tfi.ID_PACIENTE,tdp.APELLIDO_NOMBRE_PACIENTE ,tdsc.SERVICIO_ORIGEN , tdp.MAIL_1, datepart (w,tfi.FECHA_ALTA_MEDICA ) """,
}

def main():
    conexion_bd = None  # Initialize conexion_bd before the try block
    try:
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__

        configuracion = cConfiguracion.Configuracion.cargar()
        conexion_bd = cBaseDatos.BaseDatos(configuracion['database'])
        conexion_bd.conectar()
        diccionario_links = conexion_bd.obtener_diccionario_links()

        encuesta = cEncuesta.Encuesta(conexion_bd.connection, configuracion, diccionario_querys, diccionario_links, env)
        encuesta.enviar_correos()

        print("Terminado envío de encuestas")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conexion_bd:
            conexion_bd.desconectar()
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__

if __name__ == "__main__":
    main()