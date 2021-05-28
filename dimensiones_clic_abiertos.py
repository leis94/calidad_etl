import pandas as pd
import numpy as np
from pandas.io.sql import table_exists
from config import Conexion


class DimensionesClic():

    def __init__(self):
        self.dim_usuario = {}
        self.dim_prioridad = {}
        self.dim_categoria = {}
        self.dim_estado = {}
        self.dim_grupo = {}
        self.dim_asignatario = {}
        self.dim_departamento = {}
        self.dim_impacto = {}
        self.dim_resolucion = {}
        self.dim_tipo = {}
        self.dim_dependencia = {}
        self.dim_cumplimiento = {}

        self.cargar_dimesniones()

    def cargar_dimesniones(self):
        conn = Conexion()
        list_dfs = []
        for dimension in self.__dict__.keys():
            df_sql = pd.read_sql(conn.select_table_query(
                '*', dimension), conn.conecction_db())
            list_dfs.append(df_sql)

        self.dim_usuario = list_dfs[0]
        self.dim_prioridad = list_dfs[1]
        self.dim_categoria = list_dfs[2]
        self.dim_estado = list_dfs[3]
        self.dim_grupo = list_dfs[4]
        self.dim_asignatario = list_dfs[5]
        self.dim_departamento = list_dfs[6]
        self.dim_impacto = list_dfs[7]
        self.dim_resolucion = list_dfs[8]
        self.dim_tipo = list_dfs[9]
        self.dim_dependencia = list_dfs[10]
        self.dim_cumplimiento = list_dfs[11]
