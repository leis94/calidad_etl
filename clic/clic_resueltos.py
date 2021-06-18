import os
import pandas as pd
from config.config import Conexion
from config.utils import try_catch_decorator, mover_archivo, path_leaf, trim_all_columns

conn = Conexion()


@try_catch_decorator
def clic_resueltos():

    path = f"{os.path.abspath(os.getcwd())}/planos/entradas/resueltos.xlsx"

    df_excel = pd.read_excel(path)

    file_name = path_leaf(path)
    mover_archivo(file_name)

    # Convertir los formatos object en formatos strings
    df_excel = df_excel.convert_dtypes()

    df_excel = df_excel.drop_duplicates(subset=['TIQUETE'])

    # Elimino la ultima fila del excel dado que la ultima fila contiene una fila de totales que no se requiere
    df_excel = df_excel[:-1]

    df_excel = trim_all_columns(df_excel)

    # Seleciono la lista de los grupos que solamente se requieren cargar en la operación y creo un nuevo df con esos en especificos.
    grupos = ['CLARO_TELECOMUNICACIONES', 'CLARO_TELEFONIA']
    df_excel_filtered = df_excel[df_excel['GRUPO'].isin(grupos)]

    columns_bd = conn.select_columns_table(table='clic_resueltos')

    # Cambio las columnas del dataframe que están con espacios por _ las cuales son las de la BD.
    df_excel_filtered.columns = df_excel_filtered.columns[:0].tolist(
    ) + columns_bd

    df_sql = pd.read_sql(conn.select_table_limit_query(
        table='clic_resueltos'), conn.conecction_db())

    if not df_sql.empty:
        conn.truncate_table('clic_resueltos')

    df_excel_filtered.to_sql('clic_resueltos', con=conn.conecction_db(),
                             if_exists='append', index=False)
