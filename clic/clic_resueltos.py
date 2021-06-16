import os
import pandas as pd
from config.config import Conexion
from config.utils import try_catch_decorator, mover_archivo, path_leaf

conn = Conexion()


@try_catch_decorator
def clic_resueltos():

    path = f"{os.path.abspath(os.getcwd())}/planos/entradas/resueltos.xlsx"

    df_excel = pd.read_excel(path)

    file_name = path_leaf(path)
    mover_archivo(file_name)

    # Convertir los formatos object en formatos strings
    df_excel = df_excel.convert_dtypes()

    columns_names_sql = """
    SHOW columns FROM calidad_etl.clic_resueltos;
    """
    df_sql = pd.read_sql(columns_names_sql, conn.conecction_db())

    columns_clic = df_sql.iloc[0:, [0]]

    columns_clic = columns_clic['Field'].tolist()

    # Cambio las columnas del dataframe que están con espacios por _ las cuales son las de la BD.
    df_excel.columns = df_excel.columns[:0].tolist() + columns_clic

    clic_resueltos_sql = """SELECT * FROM calidad_etl.clic_resueltos LIMIT 1;"""

    df_sql = pd.read_sql(clic_resueltos_sql, conn.conecction_db())

    if not df_sql.empty:
        conn.truncate_table('clic_resueltos')

    df_excel.to_sql('clic_resueltos', con=conn.conecction_db(),
                    if_exists='append', index=False)
