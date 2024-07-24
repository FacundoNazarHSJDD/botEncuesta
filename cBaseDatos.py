import pyodbc

class BaseDatos:
    def __init__(self, configuracion):
        self.connection_string = (
            f"DRIVER={{SQL Server}};SERVER={configuracion['server']};DATABASE={configuracion['database']};"
            f"UID={configuracion['username']};PWD={configuracion['password']}"
        )
        self.connection = None

    def conectar(self):
        try:
            self.connection = pyodbc.connect(self.connection_string)
        except Exception as e:
            print(f"Error al conectar a la base de datos: {e}")

    def desconectar(self):
        if self.connection:
            self.connection.close()

    def obtener_diccionario_links(self):
        try:
            cursor = self.connection.cursor()
            query_links = "SELECT * FROM DW.dbo.Dw_Links_Encuestas"
            cursor.execute(query_links)

            diccionario_links = {}
            for row in cursor.fetchall():
                diccionario_links[row.Encuesta] = row.Link
            return diccionario_links
        except Exception as e:
            print(f"Error al obtener el diccionario de links: {e}")
            return {}
