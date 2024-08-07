SELECT   tdp.MAIL_1 AS MAIL_PACIENTE
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
	group by tfi.ID_PACIENTE,tdp.APELLIDO_NOMBRE_PACIENTE ,tdsc.SERVICIO_ORIGEN , tdp.MAIL_1, datepart (w,tfi.FECHA_ALTA_MEDICA )