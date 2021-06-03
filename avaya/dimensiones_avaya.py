import pandas as pd
from config.config import Conexion
from config.utils import trim_all_columns


class DimensionesAvaya():

    def __init__(self):
        self.dim_skill = {}
        self.dim_intervalo = {}

        self.cargar_dimensiones()

    def cargar_dimensiones(self):
        conn = Conexion()
        list_dfs = []
        for dimension in self.__dict__.keys():
            df_sql = pd.read_sql(conn.select_table_query(
                '*', dimension), conn.conecction_db())
            df_sql = trim_all_columns(df_sql)

            list_dfs.append(df_sql)

        self.dim_skill = list_dfs[0]
        self.dim_intervalo = list_dfs[1]
