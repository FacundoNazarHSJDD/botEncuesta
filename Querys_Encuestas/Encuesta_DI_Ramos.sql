SELECT   tdp.MAIL_1 AS MAIL_PACIENTE
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
--and tfr.ID_PACIENTE not in (select id_paciente from dw.dbo.Dw_Registro_Encuestas s where convert(date,s.Fecha_Envio) = convert(date,getdate()))        
	group by tfr.ID_PACIENTE , tdp.MAIL_1 ,datepart (w,FECHA_HORA_RECEP)
	    , tdp.APELLIDO_NOMBRE_PACIENTE , tdsc.SERVICIO_ORIGEN  