SELECT   tdp.MAIL_1 AS MAIL_PACIENTE
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
