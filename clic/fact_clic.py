import pandas as pd
import numpy as np
from pandas.core.frame import DataFrame
from config.config import Conexion
from clic.dimensiones_clic import DimensionesClic
from config.utils import trim_all_columns

conn = Conexion()


def fact_clic_abiertos():
    dims_clic = DimensionesClic()
    df_sql = pd.read_sql(conn.select_table_query(
        '*', 'clic_abiertos'), conn.conecction_db())

    df_sql = trim_all_columns(df_sql)

    dim_clic_abiertos = DimensionesClic()

    df_clic_abiertos = df_sql.convert_dtypes().sort_values(by='FECHA_DE_APERTURA')

    # Hago una lista con las columnas que son del tipo string para llenarlas de ND para que crucen en el merge.
    str_cols = df_clic_abiertos.columns[df_clic_abiertos.dtypes == 'string']
    df_clic_abiertos[str_cols] = df_clic_abiertos[str_cols].fillna('ND')

    # Elimino el valor ND dado que debo convertir int el df para compararlo.
    dim_usuario = dim_clic_abiertos.dim_usuario
    dim_usuario = dim_usuario[dim_usuario.CASO_USUARIO != 'ND'].astype('int64')

    # Hago el merge de los dataframes vs las dimensiones por sus campos que comparten.
    df_fact_clic_abiertos = pd.merge(df_clic_abiertos, dim_usuario, on='CASO_USUARIO').merge(dims_clic.dim_prioridad, on='PRIORIDAD').merge(
        dims_clic.dim_categoria_clic, on='CATEGORIA').merge(dims_clic.dim_estado, on='ESTADO').merge(dims_clic.dim_grupo, on='GRUPO').merge(
            dims_clic.dim_asignatario, on='ASIGNATARIO').convert_dtypes()

    # Elimino las columnas que sobran del DF dejando solo sus IDs.
    df_fact_clic_abiertos = df_fact_clic_abiertos.drop(
        ['CASO_USUARIO', 'PRIORIDAD', 'CATEGORIA', 'ESTADO', 'GRUPO', 'ASIGNATARIO'], axis=1)

    # Creo la columna "INSERTAR_DT" con la fecha de hoy con la que se insertará al cubo
    today = pd.Timestamp("today").strftime("%Y-%m-%d")

    df_fact_clic_abiertos['INSERTAR_DT'] = today

    # Parseo fecha que esta en formato obj a datetime.
    df_fact_clic_abiertos['INSERTAR_DT'] = pd.to_datetime(
        df_fact_clic_abiertos['INSERTAR_DT'])

    df_fact_clic_abiertos.rename(
        columns={"INCIDENTE_NUM": 'TIQUETE'}, inplace=True)

    df_sql = pd.read_sql(conn.select_table_limit_query(
        table='fact_clic_abiertos'), conn.conecction_db())

    if not df_sql.empty:
        conn.truncate_table('fact_clic_abiertos')

    df_fact_clic_abiertos.to_sql('fact_clic_abiertos', con=conn.conecction_db(),
                                 if_exists='append', index=False)


def fact_clic_resueltos():
    dims_clic = DimensionesClic()
    df_sql = pd.read_sql(conn.select_table_query(
        '*', 'clic_resueltos'), conn.conecction_db())

    df_clic_resueltos = df_sql.convert_dtypes().sort_values(by='FECHA_APERTURA')

    df_clic_resueltos = trim_all_columns(df_clic_resueltos)

    df_clic_resueltos['ASIGNATARIO'] = df_clic_resueltos['ASIGNATARIO'].fillna(
        'ND')
    df_clic_resueltos['ESTADO'] = df_clic_resueltos['ESTADO'].fillna('ND')
    df_clic_resueltos['PRIORIDAD'] = df_clic_resueltos['PRIORIDAD'].fillna(
        'ND')
    df_clic_resueltos['GRUPO'] = df_clic_resueltos['GRUPO'].fillna('ND')
    df_clic_resueltos['DEPARTAMENTO'] = df_clic_resueltos['DEPARTAMENTO'].fillna(
        'ND')
    df_clic_resueltos['SERVICIO_AFECTADO'] = df_clic_resueltos['SERVICIO_AFECTADO'].fillna(
        'ND')
    df_clic_resueltos['IMPACTO'] = df_clic_resueltos['IMPACTO'].fillna('ND')
    df_clic_resueltos['URGENCIA'] = df_clic_resueltos['URGENCIA'].fillna('ND')
    df_clic_resueltos['CODIGO_DE_RESOLUCION'] = df_clic_resueltos['CODIGO_DE_RESOLUCION'].fillna(
        'ND')
    df_clic_resueltos['METODO_DE_RESOLUCION'] = df_clic_resueltos['METODO_DE_RESOLUCION'].fillna(
        'ND')
    df_clic_resueltos['TIPO'] = df_clic_resueltos['TIPO'].fillna('ND')
    df_clic_resueltos['DEPENDENCIA'] = df_clic_resueltos['DEPENDENCIA'].fillna(
        'ND')
    df_clic_resueltos['REGIONAL'] = df_clic_resueltos['REGIONAL'].fillna('ND')
    df_clic_resueltos['CUMPLIMIENTO'] = df_clic_resueltos['CUMPLIMIENTO'].fillna(
        'ND')

    # Hago el merge de los dataframes vs las dimensiones por sus campos que comparten.
    df_fact_clic_resueltos = pd.merge(df_clic_resueltos, dims_clic.dim_asignatario, on='ASIGNATARIO').merge(dims_clic.dim_estado, on='ESTADO').merge(
        dims_clic.dim_prioridad, on='PRIORIDAD').merge(dims_clic.dim_grupo, on='GRUPO').merge(dims_clic.dim_departamento, on=['DEPARTAMENTO', 'SERVICIO_AFECTADO']).merge(dims_clic.dim_impacto, on=['IMPACTO', 'URGENCIA']).merge(dims_clic.dim_resolucion, on=['CODIGO_DE_RESOLUCION', 'METODO_DE_RESOLUCION']).merge(dims_clic.dim_tipo, on='TIPO').merge(dims_clic.dim_dependencia, on=['DEPENDENCIA', 'REGIONAL']).merge(
            dims_clic.dim_cumplimiento, on='CUMPLIMIENTO')

    # Elimino las columnas que sobran del DF dejando solo sus IDs y las requeridas en el CUBO de acuerdo al excel
    df_fact_clic_resueltos = df_fact_clic_resueltos.drop(
        ['ESTADO', 'PRIORIDAD', 'ASIGNATARIO', 'GRUPO', 'DEPARTAMENTO', 'SERVICIO_AFECTADO', 'IMPACTO', 'URGENCIA', 'CODIGO_DE_RESOLUCION',
         'METODO_DE_RESOLUCION', 'TIPO', 'DEPENDENCIA', 'REGIONAL', 'CUMPLIMIENTO'], axis=1)

    #import pdb; pdb.set_trace()
    df_fact_clic_resueltos_bd = pd.read_sql(conn.select_table_query(
        column='*', table='fact_clic_resueltos'), conn.conecction_db())

    df_fact_clic_resueltos_bd = df_fact_clic_resueltos_bd.drop(
        ['ID_FACT_CLIC_RESUELTOS', 'INSERTAR_DT'], axis=1)

    df_fact_clic_resueltos_bd['FECHA_APERTURA'] = df_fact_clic_resueltos_bd.FECHA_APERTURA.astype(
        np.datetime64)
    df_fact_clic_resueltos_bd['ULTIMA_ACTUALIZACION'] = df_fact_clic_resueltos_bd.ULTIMA_ACTUALIZACION.astype(
        np.datetime64)
    df_fact_clic_resueltos_bd['FECHA_RESOLUCION'] = df_fact_clic_resueltos_bd.FECHA_RESOLUCION.astype(
        np.datetime64)
    df_fact_clic_resueltos_bd['FECHA_CIERRE'] = df_fact_clic_resueltos_bd.FECHA_CIERRE.astype(
        np.datetime64)

    df_fact_clic_resueltos_bd['TIEMPO_ABIERTO'] = df_fact_clic_resueltos_bd.TIEMPO_ABIERTO.astype(
        object)
    df_fact_clic_resueltos_bd['TIEMPO_ESPERA_CLIENTE_HS'] = df_fact_clic_resueltos_bd.TIEMPO_ESPERA_CLIENTE_HS.astype(
        object)

    # Logica para comparar dos dataframes y encontrar las diferencias que se encuentran solo en el de la izquierda (al stagin area)
    df_merge_left = df_fact_clic_resueltos.merge(
        df_fact_clic_resueltos_bd, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']

    df_merge_left = df_merge_left.drop(['_merge'], axis=1)

    df_merge_left.reset_index(drop=True, inplace=True)

    tiquets_list = df_merge_left['TIQUETE'].tolist()

    for tiquete in tiquets_list:
        conn.delete_tiquete(table='fact_clic_resueltos', tiquete=tiquete)

    # Creo la columna "INSERTAR_DT" con la fecha de hoy con la que se insertará al cubo
    today = pd.Timestamp("today").strftime("%Y-%m-%d")

    df_fact_clic_resueltos = df_merge_left

    df_fact_clic_resueltos['INSERTAR_DT'] = today

    # Parseo fecha que esta en formato obj a datetime.
    df_fact_clic_resueltos['INSERTAR_DT'] = pd.to_datetime(
        df_fact_clic_resueltos['INSERTAR_DT'])

    df_fact_clic_resueltos.to_sql('fact_clic_resueltos', con=conn.conecction_db(),
                                  if_exists='append', index=False)
