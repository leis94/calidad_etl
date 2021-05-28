import pandas as pd
import numpy as np
from config import Conexion

conn = Conexion()


def run():

    df_excel = pd.read_excel(
        r'C:\Users\Cristian Silva\Documents\Repositorios\etl\data\CLIC RESUELTOS 26-04-2021.xlsx')

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

    # print(df_excel)
    df_excel.to_sql('clic_resueltos', con=conn.conecction_db(),
                    if_exists='append', index=False)


if __name__ == '__main__':
    run()
