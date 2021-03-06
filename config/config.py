import sqlalchemy as sql
import pandas as pd
import os


class Conexion():

    def __init__(self):
        self.database_type = os.getenv('DBTYPE')
        self.user = os.getenv('DBUSER')
        self.password = os.getenv('DBPASS')
        self.host = os.getenv('DBHOST')
        self.database = os.getenv('DBNAME')

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

    def select_table_date_query(self, column, table, date):

        conn = self.conecction_db()

        result = f"SELECT {column} FROM {table} WHERE FECHA = '{date}';"

        return result

    def select_table_limit_query(self, table):

        conn = self.conecction_db()

        result = f"SELECT * FROM {table} LIMIT 1;"

        return result

    def select_columns_table(self, table):

        #table = table.upper()

        columns_names_sql = f"SHOW columns FROM calidad_etl.{table};"

        df_columnas = pd.read_sql(columns_names_sql, self.conecction_db())

        #df_columnas = df_columnas.iloc[1:, [0]]

        df_columnas = df_columnas['Field'].values.tolist()

        return df_columnas

    def delete_data(self, table, date):

        conn = self.conecction_db()

        conn.execute(f"DELETE FROM {table} WHERE INSERTAR_DT = '{date}'")

        return (f"La fecha {date} fue borrada de la tabla {table}")


    def delete_tiquete(self, table, tiquete):

        conn = self.conecction_db()

        conn.execute(f"DELETE FROM {table} WHERE TIQUETE = '{tiquete}'")

        return (f"El Tiquete {tiquete} fue borrado de la tabla {table}")

    def delete_llave_unica(self, table, tiquete):

        conn = self.conecction_db()

        conn.execute(f"DELETE FROM {table} WHERE LLAVE_GENERAL_Calc = '{tiquete}'")

        return (f"El Tiquete {tiquete} fue borrado de la tabla {table}")


    def delete_avaya(self, table, fecha, id_dim_skill, id_dim_intervalo):

        conn = self.conecction_db()

        conn.execute(f"DELETE FROM {table} WHERE FECHA = '{fecha}' AND ID_DIM_SKILL = {id_dim_skill} AND ID_DIM_INTERVALO = {id_dim_intervalo}")

        return (f"El registro fue borrado de la tabla {table}")


    def select_table_query_existent(self, table, fecha, id_dim_skill, id_dim_intervalo):

        sql = f"SELECT * FROM {table} WHERE FECHA = '{fecha}' AND ID_DIM_SKILL = {id_dim_skill} AND ID_DIM_INTERVALO = {id_dim_intervalo}"

        #df = pd.read_sql(sql, self.conecction_db())

        return sql

