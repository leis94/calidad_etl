import pandas as pd
from config.config import Conexion
from config.utils import xls_2_xlsx, conver_coma_to_punto_and_float

conn = Conexion()


def av_llamadas():

    columns_bd = conn.select_columns_table(table='av_llamadas')
    # print(len(columns))

    df_llamadas = pd.read_excel(
        r'C:\Users\Cristian Silva\Documents\Repositorios\etl\data\Llamadas.xlsx')

    df_llamadas = df_llamadas.convert_dtypes()
    df_llamadas = df_llamadas.drop(0)

    # Cambio las columnas del dataframe que están con espacios por _ las cuales son las de la BD.
    df_llamadas.columns = df_llamadas.columns[:0].tolist(
    ) + columns_bd

    # Cambio el formato string a datetime
    df_llamadas['FECHA'] = pd.to_datetime(
        df_llamadas['FECHA'], format='%d/%m/%Y')

    # Cambio el formato de la columna que esta HH:MM AM al formato de 24 horas HH:MM:SS
    df_llamadas['INICIO_INTERVALO'] = pd.to_datetime(
        df_llamadas['INICIO_INTERVALO']).dt.strftime('%H:%M:%S')

    # Llamo a la función conver.... para cambiar la , por el . para y convertir el valor a float.
    df_llamadas = conver_coma_to_punto_and_float(df=df_llamadas, column=(
        '%_NIVEL_PRODUCTIVIDAD', 'PROM_AGENTES_PRESENTES', '%_NIVEL_EFICIENCIA', '%_NIVEL_SERVICIO'))

    df_sql = pd.read_sql(conn.select_table_limit_query(
        table='av_llamadas'), conn.conecction_db())

    if not df_sql.empty:
        conn.truncate_table('av_llamadas')

    df_llamadas.to_sql(
        'av_llamadas', con=conn.conecction_db(), if_exists='append', index=False)
