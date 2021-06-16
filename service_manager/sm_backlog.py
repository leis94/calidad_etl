from numpy.lib.function_base import append
import pandas as pd
import numpy as np
import os
from config.config import Conexion
from config.utils import mover_archivo, path_leaf, try_catch_decorator


conn = Conexion()


@try_catch_decorator
def sm_backlog():

    path = path = f"{os.path.abspath(os.getcwd())}/planos/entradas/prueba_backlog.xlsx"
    df_exel_backlog = pd.read_excel(path)

    file_name = path_leaf(path)
    mover_archivo(file_name)

    columns_names_sql = """SHOW columns FROM calidad_etl.sm_backlog;"""
    df_sql = pd.read_sql(columns_names_sql, conn.conecction_db())
    columns_bd = df_sql.iloc[0:, [0]]
    columns_bd = columns_bd['Field'].tolist()
    # Cambio las columnas del dataframe que están con espacios por _ las cuales son las de la BD.
    df_exel_backlog.columns = df_exel_backlog.columns[:0].tolist(
    ) + columns_bd
    sm_backlog_sql = """SELECT * FROM calidad_etl.sm_backlog LIMIT 1;"""
    df_sql = pd.read_sql(sm_backlog_sql, conn.conecction_db())
    if not df_sql.empty:
        conn.truncate_table('sm_backlog')

    df_exel_backlog.to_sql(
        'sm_backlog', con=conn.conecction_db(), if_exists='append', index=False)
