from numpy.lib.function_base import append
import pandas as pd
import numpy as np
from config import Conexion

conn = Conexion()


def run():

    df_excel_telecomunicaciones = pd.read_excel(
        r'C:\Users\Cristian Silva\Documents\Repositorios\etl\data\telecomunicaciones.xlsx')
    df_excel_telefonia = pd.read_excel(
        r'C:\Users\Cristian Silva\Documents\Repositorios\etl\data\telefonia.xlsx')

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

    df_excel_abiertos.to_sql(
        'clic_abiertos', con=conn.conecction_db(), if_exists='append', index=False)


if __name__ == '__main__':
    run()
