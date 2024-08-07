SELECT MAIL_1 AS MAIL_PACIENTE, ID_PACIENTE, APELLIDO_NOMBRE_PACIENTE, SERVICIO AS SERVICIO_ORIGEN
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