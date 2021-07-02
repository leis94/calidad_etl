import pandas as pd
from pandas.core.frame import DataFrame
from config.config import Conexion
import os

conn = Conexion()


def excluyente_proactivo():

    path = f"{os.path.abspath(os.getcwd())}/planos/entradas/excluyente_proactivo.xlsx"
    df_exel_backlog = pd.read_excel(path)
    columns_names_sql = """SHOW columns FROM calidad_etl.dim_usuarios_insertado;"""
    df_sql = pd.read_sql(columns_names_sql, conn.conecction_db())
    columns_bd = df_sql.iloc[0:, [0]]
    columns_bd = columns_bd['Field'].tolist()
    # Cambio las columnas del dataframe que están con espacios por _ las cuales son las de la BD.
    df_exel_backlog.columns = df_exel_backlog.columns[:0].tolist(
    ) + columns_bd
    sm_backlog_sql = """SELECT * FROM calidad_etl.dim_usuarios_insertado LIMIT 1;"""
    df_sql = pd.read_sql(sm_backlog_sql, conn.conecction_db())

    if not df_sql.empty:
        conn.truncate_table('dim_usuarios_insertado')
    df_exel_backlog.to_sql(
        'dim_usuarios_insertado', con=conn.conecction_db(), if_exists='append', index=False)
