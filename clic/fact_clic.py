import pandas as pd
import numpy as np
from config.config import Conexion
from clic.dimensiones_clic import DimensionesClic
from config.utils import trim_all_columns

conn = Conexion()


def fact_clic_abiertos():
    df_sql = pd.read_sql(conn.select_table_query(
        '*', 'clic_abiertos'), conn.conecction_db())

    df_sql = trim_all_columns(df_sql)

    dim_clic_abiertos = DimensionesClic()

    df_clic_abiertos = df_sql.convert_dtypes().sort_values(by='FECHA_DE_APERTURA')

    df_clic_abiertos = df_clic_abiertos.fillna('ND')

    # Elimino el valor ND dado que debo convertir int el df para compararlo.
    dim_usuario = dim_clic_abiertos.dim_usuario
    dim_usuario = dim_usuario[dim_usuario.CASO_USUARIO != 'ND'].astype('int64')

    # Hago el merge de los dataframes vs las dimensiones por sus campos que comparten.
    df_fact_clic_abiertos = pd.merge(df_clic_abiertos, dim_usuario, on='CASO_USUARIO').merge(dim_clic_abiertos.dim_prioridad, on='PRIORIDAD').merge(
        dim_clic_abiertos.dim_categoria_clic, on='CATEGORIA').merge(dim_clic_abiertos.dim_estado, on='ESTADO').merge(dim_clic_abiertos.dim_grupo, on='GRUPO').merge(
            dim_clic_abiertos.dim_asignatario, on='ASIGNATARIO').convert_dtypes()

    # Elimino las columnas que sobran del DF dejando solo sus IDs.
    df_fact_clic_abiertos = df_fact_clic_abiertos.drop(
        ['ES_PADRE', 'CASO_USUARIO', 'PRIORIDAD', 'CATEGORIA', 'ESTADO', 'GRUPO', 'ASIGNATARIO'], axis=1)

    # Creo la columna "INSERTAR_DT" con la fecha de hoy con la que se insertará al cubo
    today = pd.Timestamp("today").strftime("%Y-%m-%d")
    df_fact_clic_abiertos['INSERTAR_DT'] = today

    # Parseo fecha que esta en formato obj a datetime.
    df_fact_clic_abiertos['INSERTAR_DT'] = pd.to_datetime(
        df_fact_clic_abiertos['INSERTAR_DT'])

    df_fact_clic_abiertos.rename(
        columns={"INCIDENTE_NUM": 'TIQUETE'}, inplace=True)

    df_sql = pd.read_sql(conn.select_table_date_query(
        column='*', table='fact_clic_abiertos', date=today), conn.conecction_db())

    if not df_sql.empty:
        conn.delete_data(table='fact_clic_abiertos', date=today)

    df_fact_clic_abiertos.to_sql('fact_clic_abiertos', con=conn.conecction_db(),
                                 if_exists='append', index=False)


def fact_clic_resueltos():
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

    dim_clic_resueltos = DimensionesClic()

    # Hago el merge de los dataframes vs las dimensiones por sus campos que comparten.
    df_fact_clic_resueltos = pd.merge(df_clic_resueltos, dim_clic_resueltos.dim_asignatario, on='ASIGNATARIO').merge(dim_clic_resueltos.dim_estado, on='ESTADO').merge(
        dim_clic_resueltos.dim_prioridad, on='PRIORIDAD').merge(dim_clic_resueltos.dim_grupo, on='GRUPO').merge(dim_clic_resueltos.dim_departamento, on=['DEPARTAMENTO', 'SERVICIO_AFECTADO']).merge(dim_clic_resueltos.dim_impacto, on=['IMPACTO', 'URGENCIA']).merge(dim_clic_resueltos.dim_resolucion, on=['CODIGO_DE_RESOLUCION', 'METODO_DE_RESOLUCION']).merge(dim_clic_resueltos.dim_tipo, on='TIPO').merge(dim_clic_resueltos.dim_dependencia, on=['DEPENDENCIA', 'REGIONAL']).merge(
            dim_clic_resueltos.dim_cumplimiento, on='CUMPLIMIENTO')

    # Elimino las columnas que sobran del DF dejando solo sus IDs y las requeridas en el CUBO de acuerdo al excel
    df_fact_clic_resueltos = df_fact_clic_resueltos.drop(
        ['ESTADO', 'PRIORIDAD', 'ASIGNATARIO', 'GRUPO', 'DEPARTAMENTO', 'SERVICIO_AFECTADO', 'IMPACTO', 'URGENCIA', 'CODIGO_DE_RESOLUCION',
         'METODO_DE_RESOLUCION', 'TIPO', 'DEPENDENCIA', 'REGIONAL', 'CUMPLIMIENTO'], axis=1)

    
    # Creo la columna "INSERTAR_DT" con la fecha de hoy con la que se insertará al cubo
    today = pd.Timestamp("today").strftime("%Y-%m-%d")
    df_fact_clic_resueltos['INSERTAR_DT'] = today

    # Parseo fecha que esta en formato obj a datetime.
    df_fact_clic_resueltos['INSERTAR_DT'] = pd.to_datetime(
        df_fact_clic_resueltos['INSERTAR_DT'])

    df_sql = pd.read_sql(conn.select_table_date_query(
        column='*', table='fact_clic_resueltos', date=today), conn.conecction_db())

    if not df_sql.empty:
        conn.delete_data(table='fact_clic_resueltos', date=today)

    #import pdb; pdb.set_trace()

    df_fact_clic_resueltos.to_sql('fact_clic_resueltos', con=conn.conecction_db(),
                                  if_exists='append', index=False)
