import pandas as pd
import numpy as np
from pandas.io.sql import table_exists
from config import Conexion


class DimensionesSMC():

    def __init__(self):

        #Cerrado
        self.dim_categoria = {}
        self.dim_cliente = {}
        self.dim_grupo_sm = {}
        self.dim_servicio = {}
        self.dim_usuario_sm = {}
        #Backlog
        self.dim_categoria_backlog = {}
        self.dim_cliente_backlog = {}
        self.dim_grupo_sm_backlog = {}
        self.dim_servicio_backlog = {}
        self.dim_usuarios_sm_backlog = {}

        self.cargar_dimesniones()

    def cargar_dimesniones(self):
        conn = Conexion()
        list_dfs = []
        for dimension in self.__dict__.keys():
            df_sql = pd.read_sql(conn.select_table_query(
                '*', dimension), conn.conecction_db())
            df_sql = self.trim_all_columns(df_sql)
            list_dfs.append(df_sql)

        self.dim_categoria = list_dfs[0]
        self.dim_cliente = list_dfs[1]
        self.dim_grupo_sm = list_dfs[2]
        self.dim_servicio = list_dfs[3]
        self.dim_usuario_sm = list_dfs[4]
        self.dim_categoria_backlog = list_dfs[5]
        self.dim_cliente_backlog = list_dfs[6]
        self.dim_grupo_sm_backlog = list_dfs[7]
        self.dim_servicio_backlog = list_dfs[8]
        self.dim_usuarios_sm_backlog = list_dfs[9]
    
    def trim_all_columns(self, df):
        """
        Trim whitespace from ends of each value across all series in dataframe
        """
        trim_strings = lambda x: x.strip() if isinstance(x, str) else x
        return df.applymap(trim_strings)