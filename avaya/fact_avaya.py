import pandas as pd
import numpy as np
from config.config import Conexion
from config.utils import trim_all_columns
from .dimensiones_avaya import DimensionesAvaya

conn = Conexion()
dims_avaya = DimensionesAvaya()


def fact_av_llamadas():

    df_av_llamadas = pd.read_sql(conn.select_table_query(
        '*', 'av_llamadas'), conn.conecction_db())

    df_av_llamadas = df_av_llamadas.convert_dtypes()

    # Hago el merge de los dataframes vs las dimensiones por sus campos que comparten.
    df_fact_av_llamadas = pd.merge(df_av_llamadas, dims_avaya.dim_skill, on=['NUMERO_SKILL', 'NOMBRE_SKILL']).merge(
        dims_avaya.dim_intervalo, on=['INICIO_INTERVALO', 'FIN_INTERVALO'])

    df_fact_av_llamadas['PRODUCTO_PARA_TMO'] = df_fact_av_llamadas['LLAMADAS_ATENDIDAS'] * \
        df_fact_av_llamadas['TIEMPO_PROM_ACD']

    df_fact_av_llamadas['TIEMPO_LLAMADAS_SALIENTES'] = df_fact_av_llamadas['LLAMADAS_SALIENTES_EXTENSION'] * \
        df_fact_av_llamadas['TIEMPO_PROM_CONVER_LLAMADAS_SALIENTES_EXTENSION']

    # Creo la columna "INSERTAR_DT" con la fecha de hoy con la que se insertará al cubo
    today = pd.Timestamp("today").strftime("%Y-%m-%d")
    df_fact_av_llamadas['INSERTAR_DT'] = today

    # Parseo fecha que esta en formato obj a datetime.
    df_fact_av_llamadas['INSERTAR_DT'] = pd.to_datetime(
        df_fact_av_llamadas['INSERTAR_DT'])

    columns_bd = conn.select_columns_table('fact_av_llamadas')
    columns_bd.pop(0)

    columns_df = (df_fact_av_llamadas.columns).to_list()

    # Elimino las columnas que sobran del DF dejando solo las columnas necesarias para insertar en la BD, esto se hace con list comprenhensions.
    df_fact_av_llamadas = df_fact_av_llamadas.drop(
        [columna for columna in columns_df if columna not in columns_bd], axis=1)

    df_sql = pd.read_sql(conn.select_table_date_query(
        column='*', table='fact_av_llamadas', date=today), conn.conecction_db())

    if not df_sql.empty:
        conn.delete_data(table='fact_av_llamadas', date=today)

    df_fact_av_llamadas.to_sql('fact_av_llamadas', con=conn.conecction_db(),
                               if_exists='append', index=False)


def fact_av_abandonos():

    df_av_abandonos = pd.read_sql(conn.select_table_query(
        '*', 'av_abandonos'), conn.conecction_db())

    df_av_abandonos = df_av_abandonos.convert_dtypes()

    # Hago el merge de los dataframes vs las dimensiones por sus campos que comparten.
    df_fact_av_abandonos = pd.merge(df_av_abandonos, dims_avaya.dim_skill, left_on='SPLIT_SKILL', right_on='NUMERO_SKILL').merge(
        dims_avaya.dim_intervalo, left_on='INTERVALO', right_on='INICIO_INTERVALO')

    # Creo la columna "INSERTAR_DT" con la fecha de hoy con la que se insertará al cubo
    today = pd.Timestamp("today").strftime("%Y-%m-%d")
    df_fact_av_abandonos['INSERTAR_DT'] = today

    # Parseo fecha que esta en formato obj a datetime.
    df_fact_av_abandonos['INSERTAR_DT'] = pd.to_datetime(
        df_fact_av_abandonos['INSERTAR_DT'])

    columns_bd = conn.select_columns_table('fact_av_abandonos')
    columns_bd.pop(0)

    columns_df = (df_fact_av_abandonos.columns).to_list()

    # Elimino las columnas que sobran del DF dejando solo las columnas necesarias para insertar en la BD, esto se hace con list comprenhensions.
    df_fact_av_abandonos = df_fact_av_abandonos.drop(
        [columna for columna in columns_df if columna not in columns_bd], axis=1)

    df_sql = pd.read_sql(conn.select_table_date_query(
        column='*', table='fact_av_abandonos', date=today), conn.conecction_db())

    if not df_sql.empty:
        conn.delete_data(table='fact_av_abandonos', date=today)

    df_fact_av_abandonos.to_sql('fact_av_abandonos', con=conn.conecction_db(),
                                if_exists='append', index=False)
