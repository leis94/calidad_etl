import sqlalchemy as sql


class Conexion():

    def __init__(self):
        self.database_type = "mysql"
        self.user = 'root'
        self.password = 'admin123#'
        self.host = 'localhost:3306'
        self.database = 'calidad_etl'
        self.name = 'Andres'

    def conecction_db(self):

        conn_string = f'{self.database_type}://{self.user}:{self.password}@{self.host}/{self.database}?charset=utf8'

        engine = sql.create_engine(conn_string)

        return engine

    def truncate_table(self, table):

        conn = self.conecction_db()

        conn.execute(f"TRUNCATE TABLE {table}")

        return (f"{table}Truncada")
