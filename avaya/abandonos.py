import pandas as pd
from config.config import Conexion
from config.utils import xls_2_xlsx, conver_coma_to_punto_and_float

conn = Conexion()


def av_abandonos():

    columns_bd = conn.select_columns_table(table='av_abandonos')

    df_abandonos = pd.read_excel(
        r'C:\Users\Cristian Silva\Documents\Repositorios\etl\data\Abandonos.xlsx')

    df_abandonos = df_abandonos.convert_dtypes()

    # Cambio las columnas del dataframe que están con espacios por _ las cuales son las de la BD.
    df_abandonos.columns = df_abandonos.columns[:0].tolist(
    ) + columns_bd

    # Cambio el formato string a datetime
    df_abandonos['FECHA'] = pd.to_datetime(
        df_abandonos['FECHA'], format='%d/%m/%Y')

    df_sql = pd.read_sql(conn.select_table_limit_query(
        table='av_abandonos'), conn.conecction_db())

    if not df_sql.empty:
        conn.truncate_table('av_abandonos')

    df_abandonos.to_sql(
        'av_abandonos', con=conn.conecction_db(), if_exists='append', index=False)
