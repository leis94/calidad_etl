import pandas as pd
import numpy as np
from config.config import Conexion
from config.utils import trim_all_columns

conn = Conexion()


def dimensiones_clic_abiertos():

    clic_abiertos_sql = "SELECT * FROM calidad_etl.clic_abiertos;"

    df_sql = pd.read_sql(clic_abiertos_sql, conn.conecction_db())

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

    clic_resueltos_sql = "SELECT * FROM calidad_etl.clic_resueltos;"

    df_sql = pd.read_sql(clic_resueltos_sql, conn.conecction_db())

    df_sql = df_sql.convert_dtypes()

    # Busco los valores unicos (no repeitodos) de las columnas que serán dimensiones, como esto devuelve un np.array lo convierto en un dataframe de nuevo

    df_dim_asignatario = pd.DataFrame(
        (df_sql.loc[:, "ASIGNATARIO"].unique()), columns=["ASIGNATARIO"])

    df_dim_estado = pd.DataFrame(
        (df_sql.loc[:, "ESTADO"].unique()), columns=["ESTADO"])

    df_dim_prioridad = pd.DataFrame(
        (df_sql.loc[:, "PRIORIDAD"].unique()), columns=["PRIORIDAD"])

    df_dim_grupo = pd.DataFrame(
        (df_sql.loc[:, "GRUPO"].unique()), columns=["GRUPO"])

    df_dim_departamento = pd.DataFrame(
        (df_sql.loc[:, ["DEPARTAMENTO", "SERVICIO_AFECTADO"]].drop_duplicates()), columns=["DEPARTAMENTO", "SERVICIO_AFECTADO"])

    df_dim_impacto = pd.DataFrame(
        (df_sql.loc[:, ["IMPACTO", "URGENCIA"]].drop_duplicates()), columns=["IMPACTO", "URGENCIA"])

    df_dim_impacto.fillna('ND', inplace=True)

    df_dim_resolucion = pd.DataFrame(
        (df_sql.loc[:, ["CODIGO_DE_RESOLUCION", "METODO_DE_RESOLUCION"]].drop_duplicates()), columns=["CODIGO_DE_RESOLUCION", "METODO_DE_RESOLUCION"])

    df_dim_tipo = pd.DataFrame(
        (df_sql.loc[:, "TIPO"].drop_duplicates()), columns=["TIPO"])

    df_dim_dependencia = pd.DataFrame(
        (df_sql.loc[:, ["DEPENDENCIA", "REGIONAL"]].drop_duplicates()), columns=["DEPENDENCIA", "REGIONAL"])

    df_dim_dependencia.fillna('ND', inplace=True)

    df_dim_cumplimiento = pd.DataFrame(
        (df_sql.loc[:, "CUMPLIMIENTO"].drop_duplicates()), columns=["CUMPLIMIENTO"])

    df_dim_asignatario.dropna(axis=0, inplace=True)
    df_dim_estado.dropna(axis=0, inplace=True)
    df_dim_prioridad.dropna(axis=0, inplace=True)
    df_dim_grupo.dropna(axis=0, inplace=True)
    df_dim_departamento.dropna(axis=0, inplace=True)
    df_dim_impacto.dropna(axis=0, inplace=True)
    df_dim_resolucion.dropna(axis=0, inplace=True)
    df_dim_tipo.dropna(axis=0, inplace=True)
    df_dim_dependencia.dropna(axis=0, inplace=True)
    df_dim_cumplimiento.dropna(axis=0, inplace=True)

    dict_dfs_one_column = {"dim_asignatario": ['ASIGNATARIO', df_dim_asignatario],
                           "dim_estado": ['ESTADO', df_dim_estado],
                           "dim_prioridad": ['PRIORIDAD', df_dim_prioridad],
                           "dim_grupo": ['GRUPO', df_dim_grupo],
                           "dim_tipo": ['TIPO', df_dim_tipo],
                           "dim_cumplimiento": ['CUMPLIMIENTO', df_dim_cumplimiento]
                           }

    dict_dfs_several_columns = {"dim_departamento": ["DEPARTAMENTO", "SERVICIO_AFECTADO", df_dim_departamento],
                                "dim_impacto": ["IMPACTO", "URGENCIA", df_dim_impacto],
                                "dim_resolucion": ["CODIGO_DE_RESOLUCION", "METODO_DE_RESOLUCION", df_dim_resolucion],
                                "dim_dependencia": ["DEPENDENCIA", "REGIONAL", df_dim_dependencia]
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

        dim_sql = f"SELECT {atributos_and_df[0]} FROM calidad_etl.{table} WHERE id_{table} <> 1;"

        df_dim_bd = pd.read_sql(dim_sql, conn.conecction_db())

        # Convierto los tipo objectos del df en strings y por ultimo ordeno por la columna para compararlos.
        df_dim_bd = df_dim_bd.convert_dtypes().sort_values(
            by=f'{atributos_and_df[0]}')

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

    dict_dfs = {}

    for table, atributos_and_df in dfs.items():

        # Convierto las columnas en obj de string de pandas, luego convierto los objectos en strings y por ultimo ordeno por la columna para compararlos.

        df = atributos_and_df[2].applymap(
            str).convert_dtypes().sort_values(by=atributos_and_df[0])

        # Llamar a la función para hacer str a los parametros strings del dataframe
        df = trim_all_columns(df)

        # Pongo este condificional por la casuistica con la tabla dim_dependencia donde filtro desde ND
        if table == 'dim_dependencia':
            dim_sql = f"SELECT {atributos_and_df[0]}, {atributos_and_df[1]} FROM calidad_etl.{table};"

        else:
            dim_sql = f"SELECT {atributos_and_df[0]}, {atributos_and_df[1]} FROM calidad_etl.{table} WHERE id_{table} <> 1;"

        #dim_sql = f"SELECT {atributos_and_df[0]}, {atributos_and_df[1]} FROM calidad_etl.{table} WHERE id_{table} <> 1;"

        df_dim_bd = pd.read_sql(dim_sql, conn.conecction_db())

        # Convierto los tipo objectos del df en strings y por ultimo ordeno por la columna para compararlos.
        df_dim_bd = df_dim_bd.convert_dtypes().sort_values(
            by=f'{atributos_and_df[0]}')

        # Llamar a la función para hacer str a los parametros strings del dataframe
        df_dim_bd = trim_all_columns(df_dim_bd)

        # Logica para comparar dos dataframes y encontrar las diferencias que se encuentran solo en el de la izquierda (al stagin area)
        df_merge_left = df.merge(
            df_dim_bd, indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']

        df_merge_left = df_merge_left.iloc[0:, [0, 1]]
        df_merge_left.reset_index(drop=True, inplace=True)

        if not df_merge_left.empty:

            dict_dfs[f'{table}'] = df_merge_left

    return dict_dfs
