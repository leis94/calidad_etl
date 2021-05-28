import sqlalchemy as sql
import pandas as pd

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

        return (f"{table} Truncada")

    
    def select_table_query(self, column, table):

        conn = self.conecction_db()

        result = f"SELECT {column} FROM {table};"

        return result


    def select_columns_table(self, table):

        table = table.upper()
        
        columns_names_sql = f"SHOW columns FROM calidad_etl.{table};"

        df_columnas = pd.read_sql(columns_names_sql, self.conecction_db())

        df_columnas = df_columnas.iloc[1:,[0]]

        df_columnas = df_columnas['Field'].values.tolist()

        #print(df_columnas)

        # df_columnas = df_columnas.tolist()

        # df_columnas = df_columnas.iloc[1:[0]].tolist()

        return df_columnas
