import pandas as pd
import numpy as np
from config.config import Conexion
from config.utils import trim_all_columns
from .dimensiones_clic import DimensionesClic

conn = Conexion()


def dimensiones_clic_abiertos():

    df_sql = pd.read_sql(conn.select_table_query(
        column='*', table="clic_abiertos"), conn.conecction_db())

    df_sql = trim_all_columns(df_sql)

    df_sql = df_sql.convert_dtypes()

    # Busco los valores unicos (no repetidos) de las columnas que serán dimensiones, como esto devuelve un np.array lo convierto en un dataframe de nuevo
    df_dim_usuario = pd.DataFrame(
        (df_sql.loc[:, "CASO_USUARIO"].unique()), columns=["CASO_USUARIO"])
    df_dim_prioridad = pd.DataFrame(
        (df_sql.loc[:, "PRIORIDAD"].unique()), columns=["PRIORIDAD"])

    df_dim_categoria_clic = pd.DataFrame(
        (df_sql.loc[:, "CATEGORIA"].unique()), columns=["CATEGORIA"])

    df_dim_estado = pd.DataFrame(
        (df_sql.loc[:, "ESTADO"].unique()), columns=["ESTADO"])
    df_dim_grupo = pd.DataFrame(
        (df_sql.loc[:, "GRUPO"].unique()), columns=["GRUPO"])
    df_dim_asignatario = pd.DataFrame(
        (df_sql.loc[:, "ASIGNATARIO"].unique()), columns=["ASIGNATARIO"])

    df_dim_usuario.dropna(axis=0, inplace=True)
    df_dim_prioridad.dropna(axis=0, inplace=True)
    df_dim_categoria_clic.dropna(axis=0, inplace=True)
    df_dim_estado.dropna(axis=0, inplace=True)
    df_dim_grupo.dropna(axis=0, inplace=True)
    df_dim_asignatario.dropna(axis=0, inplace=True)

    dict_dfs = {"dim_usuario": ['CASO_USUARIO', df_dim_usuario],
                "dim_prioridad": ['PRIORIDAD', df_dim_prioridad],
                "dim_categoria_clic": ['CATEGORIA', df_dim_categoria_clic],
                "dim_estado": ['ESTADO', df_dim_estado],
                "dim_grupo": ['GRUPO', df_dim_grupo],
                "dim_asignatario": ['ASIGNATARIO', df_dim_asignatario]
                }

    # Llamo la función de comparar dimensiones vs valores nuevos provenientes en el excel.
    dataframes = comparar_dimensiones_vs_valores_nuevos(dict_dfs)

    for table, df in dataframes.items():

        print(table)
        print(df)
        df.to_sql(f'{table}', con=conn.conecction_db(),
                  if_exists='append', index=False)
        print(f'Inserto en la tabla: {table}')


def dimensiones_clic_resueltos():

    df_resueltos = pd.read_sql(conn.select_table_query(
        column='*', table="clic_resueltos"), conn.conecction_db())

    df_resueltos = trim_all_columns(df_resueltos)

    # Busco los valores unicos (no repeitodos) de las columnas que serán dimensiones, como esto devuelve un np.array lo convierto en un dataframe de nuevo

    df_dim_asignatario = pd.DataFrame(
        (df_resueltos.loc[:, "ASIGNATARIO"].unique()), columns=["ASIGNATARIO"])

    df_dim_asignatario = df_dim_asignatario.drop_duplicates(
    ).reset_index(drop=True).convert_dtypes()

    df_dim_estado = pd.DataFrame(
        (df_resueltos.loc[:, "ESTADO"].unique()), columns=["ESTADO"])

    df_dim_estado = df_dim_estado.drop_duplicates(
    ).reset_index(drop=True).convert_dtypes()

    df_dim_prioridad = pd.DataFrame(
        (df_resueltos.loc[:, "PRIORIDAD"].unique()), columns=["PRIORIDAD"])

    df_dim_prioridad = df_dim_prioridad.drop_duplicates(
    ).reset_index(drop=True).convert_dtypes()

    df_dim_grupo = pd.DataFrame(
        (df_resueltos.loc[:, "GRUPO"].unique()), columns=["GRUPO"])

    df_dim_grupo = df_dim_grupo.drop_duplicates(
    ).reset_index(drop=True).convert_dtypes()

    df_dim_departamento = pd.DataFrame(
        (df_resueltos.loc[:, ["DEPARTAMENTO", "SERVICIO_AFECTADO"]].drop_duplicates()), columns=["DEPARTAMENTO", "SERVICIO_AFECTADO"])

    df_dim_departamento = df_dim_departamento.drop_duplicates(
    ).reset_index(drop=True).convert_dtypes()

    df_dim_departamento.fillna('ND', inplace=True)

    df_dim_impacto = pd.DataFrame(
        (df_resueltos.loc[:, ["IMPACTO", "URGENCIA"]].drop_duplicates()), columns=["IMPACTO", "URGENCIA"])

    df_dim_impacto = df_dim_impacto.drop_duplicates(
    ).reset_index(drop=True).convert_dtypes()

    df_dim_impacto.fillna('ND', inplace=True)

    df_dim_resolucion = pd.DataFrame(
        (df_resueltos.loc[:, ["CODIGO_DE_RESOLUCION", "METODO_DE_RESOLUCION"]].drop_duplicates()), columns=["CODIGO_DE_RESOLUCION", "METODO_DE_RESOLUCION"])

    df_dim_resolucion = df_dim_resolucion.drop_duplicates(
    ).reset_index(drop=True).convert_dtypes()

    df_dim_resolucion.fillna('ND', inplace=True)

    df_dim_tipo = pd.DataFrame(
        (df_resueltos.loc[:, "TIPO"].drop_duplicates()), columns=["TIPO"])

    df_dim_tipo = df_dim_tipo.drop_duplicates(
    ).reset_index(drop=True).convert_dtypes()

    df_dim_dependencia = pd.DataFrame(
        (df_resueltos.loc[:, ["DEPENDENCIA", "REGIONAL"]].drop_duplicates()), columns=["DEPENDENCIA", "REGIONAL"])

    df_dim_dependencia = df_dim_dependencia.drop_duplicates(
    ).reset_index(drop=True).convert_dtypes()

    df_dim_dependencia.fillna('ND', inplace=True)

    df_dim_cumplimiento = pd.DataFrame(
        (df_resueltos.loc[:, "CUMPLIMIENTO"].drop_duplicates()), columns=["CUMPLIMIENTO"])

    df_dim_cumplimiento = df_dim_cumplimiento.drop_duplicates(
    ).reset_index(drop=True).convert_dtypes()

    df_dim_asignatario.dropna(axis=0, inplace=True)
    df_dim_estado.dropna(axis=0, inplace=True)
    df_dim_prioridad.dropna(axis=0, inplace=True)
    df_dim_grupo.dropna(axis=0, inplace=True)
    #df_dim_departamento.dropna(axis=0, inplace=True)
    #df_dim_impacto.dropna(axis=0, inplace=True)
    #df_dim_resolucion.dropna(axis=0, inplace=True)
    df_dim_tipo.dropna(axis=0, inplace=True)

    df_dim_cumplimiento.dropna(axis=0, inplace=True)

    dict_dfs_one_column = {"dim_asignatario": ['ASIGNATARIO', df_dim_asignatario],
                           "dim_estado": ['ESTADO', df_dim_estado],
                           "dim_prioridad": ['PRIORIDAD', df_dim_prioridad],
                           "dim_grupo": ['GRUPO', df_dim_grupo],
                           "dim_tipo": ['TIPO', df_dim_tipo],
                           "dim_cumplimiento": ['CUMPLIMIENTO', df_dim_cumplimiento]
                           }

    dict_dfs_several_columns = {"dim_departamento": df_dim_departamento,
                                "dim_impacto": df_dim_impacto,
                                "dim_resolucion": df_dim_resolucion,
                                "dim_dependencia": df_dim_dependencia
                                }

    # Llamo la función de comparar dimensiones vs valores nuevos provenientes en el excel.
    dataframes = comparar_dimensiones_vs_valores_nuevos(dict_dfs_one_column)
    dataframes_two = comparar_dimensiones_dos_vs_valores_nuevos(
        dict_dfs_several_columns)

    for table, df in dataframes.items():

        df.to_sql(f'{table}', con=conn.conecction_db(),
                  if_exists='append', index=False)
        print(f'Inserto en la tabla: {table}')

    for table, df in dataframes_two.items():

        df.to_sql(f'{table}', con=conn.conecction_db(),
                  if_exists='append', index=False)
        print(f'Inserto en la tabla: {table}')


def comparar_dimensiones_vs_valores_nuevos(dfs):

    dict_dfs = {}

    for table, atributos_and_df in dfs.items():

        # Convierto las columnas en obj de string de pandas, luego convierto los objectos en strings y por ultimo ordeno por la columna para compararlos.

        df = atributos_and_df[1].applymap(
            str).convert_dtypes().sort_values(by=atributos_and_df[0])

        # Llamar a la función para hacer str a los parametros strings del dataframe
        df = trim_all_columns(df)

        #dim_sql = f"SELECT {atributos_and_df[0]} FROM calidad_etl.{table} WHERE id_{table} <> 1;"

        df_dim_bd = pd.read_sql(conn.select_table_query(
            column=f'{atributos_and_df[0]}', table=f"{table}"), conn.conecction_db())

        # Convierto los tipo objectos del df en strings y por ultimo ordeno por la columna para compararlos.
        df_dim_bd = df_dim_bd.convert_dtypes()
        # .sort_values(
        #     by=f'{atributos_and_df[0]}')

        # Llamar a la función para hacer str a los parametros strings del dataframe
        df_dim_bd = trim_all_columns(df_dim_bd)

        # Logica para comparar dos dataframes y encontrar las diferencias que se encuentran solo en el de la izquierda (al stagin area)
        df_merge_left = df.merge(
            df_dim_bd, indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']

        df_merge_left = df_merge_left.iloc[0:, [0]]

        df_merge_left.reset_index(drop=True, inplace=True)

        if not df_merge_left.empty:

            dict_dfs[f'{table}'] = df_merge_left

    return dict_dfs


def comparar_dimensiones_dos_vs_valores_nuevos(dfs):
    dims_clic = DimensionesClic()
    dict_dfs = {}

    dict_dims = {}

    for dimension, dataframe in dims_clic.__dict__.items():
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
