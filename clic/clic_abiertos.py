import os
import pandas as pd
from config.config import Conexion
from config.utils import try_catch_decorator, mover_archivo, path_leaf

conn = Conexion()


@try_catch_decorator
def clic_abiertos():

    path = f"{os.path.abspath(os.getcwd())}/planos/entradas/telecomunicaciones.xlsx"
    df_excel_telecomunicaciones = pd.read_excel(path)

    file_name = path_leaf(path)
    mover_archivo(file_name)

    path = f"{os.path.abspath(os.getcwd())}/planos/entradas/telefonia.xlsx"
    df_excel_telefonia = pd.read_excel(path)

    file_name = path_leaf(path)
    mover_archivo(file_name)

    # Convertir los formatos object en formatos strings
    df_excel_telecomunicaciones = df_excel_telecomunicaciones.convert_dtypes()
    df_excel_telefonia = df_excel_telefonia.convert_dtypes()

    df_excel_abiertos = df_excel_telefonia.append(df_excel_telecomunicaciones)

    columns_bd = conn.select_columns_table(table='clic_abiertos')

    # Cambio las columnas del dataframe que están con espacios por _ las cuales son las de la BD.
    df_excel_abiertos.columns = df_excel_abiertos.columns[:0].tolist(
    ) + columns_bd

    df_excel_abiertos = df_excel_abiertos.drop_duplicates(
        subset=['INCIDENTE_NUM'])

    df_sql = pd.read_sql(conn.select_table_limit_query(
        table='clic_abiertos'), conn.conecction_db())

    if not df_sql.empty:
        conn.truncate_table('clic_abiertos')

    df_excel_abiertos.to_sql(
        'clic_abiertos', con=conn.conecction_db(), if_exists='append', index=False)
