from numpy.lib.function_base import append
import pandas as pd
from config.config import Conexion
from config.utils import mover_archivo, path_leaf, try_catch_decorator
import os

conn = Conexion()


@try_catch_decorator
def sm_cerrado():

    path = f"{os.path.abspath(os.getcwd())}/planos/entradas/prueba_cerrado.xlsx"
    df_exel_cerrado = pd.read_excel(path)

    file_name = path_leaf(path)
    mover_archivo(file_name)

    # Seleciono la lista de los grupos que solamente se requieren cargar en la operación y creo un nuevo df con esos en especificos.
    nombre_cliente = ['BBVA COLOMBIA', 'ALMACENES EXITO S.A.', 'TUYA S.A']
    grupo_asignado = ['EYN - NOC BBVA', 'EYN - NOC EXITO']
    df_excel_filtered = df_exel_cerrado[df_exel_cerrado['NOMBRE_CLIENTE'].isin(
        nombre_cliente)]
    df_excel_filtered = df_exel_cerrado[df_exel_cerrado['GRUPO_ASIGNADO'].isin(
        grupo_asignado)]

    columns_bd = conn.select_columns_table(table='sm_cerrado')

    # Cambio las columnas del dataframe que están con espacios por _ las cuales son las de la BD.
    df_excel_filtered.columns = df_excel_filtered.columns[:0].tolist(
    ) + columns_bd

    df_sql = pd.read_sql(conn.select_table_limit_query(
        table='sm_cerrado'), conn.conecction_db())

    if not df_sql.empty:
        conn.truncate_table('sm_cerrado')

    df_excel_filtered.to_sql('sm_cerrado', con=conn.conecction_db(),
                             if_exists='append', index=False)
