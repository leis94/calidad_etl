from numpy.lib.function_base import append
import pandas as pd
import numpy as np
from config.config import Conexion
from config.utils import try_catch_decorator

conn = Conexion()

@try_catch_decorator
def clic_abiertos():

    df_excel_telecomunicaciones = pd.read_excel(
        r'C:\Users\Cristian Silva\Documents\Repositorios\etl\planos\entradas\telecomunicaciones.xlsx')
    df_excel_telefonia = pd.read_excel(
        r'C:\Users\Cristian Silva\Documents\Repositorios\etl\planos\entradas\telefonia.xlsx')

    # Convertir los formatos object en formatos strings
    df_excel_telecomunicaciones = df_excel_telecomunicaciones.convert_dtypes()
    df_excel_telefonia = df_excel_telefonia.convert_dtypes()

    df_excel_abiertos = df_excel_telefonia.append(df_excel_telecomunicaciones)

    columns_names_sql = """SHOW columns FROM calidad_etl.clic_abiertos;"""

    df_sql = pd.read_sql(columns_names_sql, conn.conecction_db())

    columns_bd = df_sql.iloc[0:, [0]]

    columns_bd = columns_bd['Field'].tolist()

    # Cambio las columnas del dataframe que están con espacios por _ las cuales son las de la BD.
    df_excel_abiertos.columns = df_excel_abiertos.columns[:0].tolist(
    ) + columns_bd

    clic_abiertos_sql = """SELECT * FROM calidad_etl.clic_abiertos LIMIT 1;"""

    df_sql = pd.read_sql(clic_abiertos_sql, conn.conecction_db())

    if not df_sql.empty:
        conn.truncate_table('clic_abiertos')

    df_excel_abiertos.to_sql(
        'clic_abiertos', con=conn.conecction_db(), if_exists='append', index=False)


# if __name__ == '__main__':
#     run()