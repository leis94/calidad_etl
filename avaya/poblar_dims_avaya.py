import pandas as pd
import numpy as np
from config.config import Conexion
from config.utils import trim_all_columns
from .dimensiones_avaya import DimensionesAvaya


conn = Conexion()
dims_avaya = DimensionesAvaya()


def dims_avaya_llamadas():

    df_llamadas = pd.read_sql(conn.select_table_query(
        column='*', table="av_llamadas"), conn.conecction_db())

    df_llamadas = trim_all_columns(df_llamadas)

    # Elimino los valores repetidos de las columnas que serán dimensiones para guardar.
    df_dim_skill = pd.DataFrame(
        (df_llamadas.loc[:, ["NUMERO_SKILL", "NOMBRE_SKILL"]]))
    df_dim_skill = df_dim_skill.drop_duplicates(
    ).reset_index(drop=True).convert_dtypes()

    df_dim_intervalo = pd.DataFrame(
        (df_llamadas.loc[:, ["INICIO_INTERVALO", "FIN_INTERVALO"]]))
    df_dim_intervalo = df_dim_intervalo.drop_duplicates().reset_index(drop=True)

    dict_dfs = {"dim_skill": df_dim_skill,
                "dim_intervalo": df_dim_intervalo
                }

    dataframes = comparar_dimensiones_vs_valores_nuevos(dict_dfs)

    for table, df in dataframes.items():

        df.to_sql(f'{table}', con=conn.conecction_db(),
                  if_exists='append', index=False)
        print(f'Inserto en la tabla: {table}')


def dims_avaya_abandonos():

    df_abandonos = pd.read_sql(conn.select_table_query(
        column='*', table="av_abandonos"), conn.conecction_db())

    df_abandonos = trim_all_columns(df_abandonos)

    # Elimino los valores repetidos de las columnas que serán dimensiones para guardar.
    df_dim_skill = pd.DataFrame(
        (df_abandonos.loc[:, ["SPLIT_SKILL"]]))
    df_dim_skill = df_dim_skill.drop_duplicates(
    ).reset_index(drop=True).convert_dtypes()

    df_dim_intervalo = pd.DataFrame(
        (df_abandonos.loc[:, ["INTERVALO"]]))
    df_dim_intervalo = df_dim_intervalo.drop_duplicates().reset_index(drop=True)

    dict_dfs = {"dim_skill": ['NUMERO_SKILL', df_dim_skill],
                "dim_intervalo": ['INICIO_INTERVALO', df_dim_intervalo]
                }

    dataframes = comparar_dimensiones_abandonos_vs_valores_nuevos(dict_dfs)

    for table, df in dataframes.items():

        df.to_sql(f'{table}', con=conn.conecction_db(),
                  if_exists='append', index=False)
        print(f'Inserto en la tabla: {table}')


def comparar_dimensiones_vs_valores_nuevos(dfs):
    dict_dfs = {}

    dict_dims = {}
    for dimension, dataframe in dims_avaya.__dict__.items():
        dimension_mayus = dimension.upper()
        dict_dims[dimension] = dataframe.drop(
            [f'ID_{dimension_mayus}'], axis=1)

    for table, df in dfs.items():

        df_merge_left = df.merge(
            dict_dims[f'{table}'], indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']

        df_merge_left = df_merge_left.drop(['_merge'], axis=1)

        if not df_merge_left.empty:

            dict_dfs[f'{table}'] = df_merge_left

    return dict_dfs


def comparar_dimensiones_abandonos_vs_valores_nuevos(dfs):
    dict_dfs = {}

    for table, columna_and_df in dfs.items():

        df = columna_and_df[1]

        # Renombro el nombre de la colmuna dado que en abandonos se llama difrente pero en la dimension y en llamadas es como viene en el dict
        df.columns = [f'{columna_and_df[0]}']

        df_dim_bd = pd.read_sql(conn.select_table_query(
            column=f'{columna_and_df[0]}', table=f"{table}"), conn.conecction_db())

        df_dim_bd = trim_all_columns(df_dim_bd)

        df_merge_left = df.merge(
            df_dim_bd, indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']

        df_merge_left = df_merge_left.drop(['_merge'], axis=1)

        if not df_merge_left.empty:

            dict_dfs[f'{table}'] = df_merge_left

    return dict_dfs
