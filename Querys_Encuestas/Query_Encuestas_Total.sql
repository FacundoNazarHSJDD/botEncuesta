SELECT 
    Encuesta,
    COUNT(*) AS Total_Pacientes
FROM 
    dw.dbo.Dw_Registro_Encuestas
WHERE 
    CONVERT(date, Fecha_Envio) = CONVERT(date, GETDATE())
GROUP BY 
    Encuesta;