import pandas as pd
import numpy as np
from pandas.core.frame import DataFrame
from config.config import Conexion
from config.utils import trim_all_columns
from .dimensiones_sm_c import DimensionesSMC
from .calculo_columna import cumplimiento, SLA_BACK_CALC, operacion, prioridad
from .calculo_columna import tecnologia, id_compania, FECHA_INFGESO_ESTADO_CORTA, EXCLUYENTE_PROACTIVO


conn = Conexion()


def fact_sm_backlog():

    df_sql = pd.read_sql(conn.select_table_query(
        '*', 'sm_backlog'), conn.conecction_db())

    if not df_sql.empty:

        prioridad()

        dim_sm_backlog = DimensionesSMC()

        df_sm_backlog = df_sql.convert_dtypes()
        df_sm_backlog = trim_all_columns(df_sm_backlog)

        df_sm_backlog_new = pd.DataFrame(
            columns=["OPERACION", "CUMPLIMIENTO_CAL", "SLA_BACK_CALC", "VENCIMIENTO"])

        df_sm_backlog = pd.concat([df_sm_backlog, df_sm_backlog_new], axis=1)

        df_sm_backlog = df_sm_backlog.fillna('ND')

        # Hago el merge de los dataframes vs las dimensiones por sus campos que comparten.
        df_fact_sm_backlog = pd.merge(df_sm_backlog,
                                      dim_sm_backlog.dim_categoria_backlog, on=["PRIORITY"]).merge(
            dim_sm_backlog.dim_cliente_backlog, on=["CUSTOMER_ID", "DEPT_ID", "DEPT"]).merge(
            dim_sm_backlog.dim_grupo_sm_backlog, on=["OPEN_GROUP", "ASSIGNED_GROUP"]).merge(
            dim_sm_backlog.dim_servicio_backlog, on="ServicesManager").merge(
            dim_sm_backlog.dim_usuarios_sm_backlog, on="OPENED_BY").convert_dtypes()

        # Elimino las columnas que sobran del DF dejando solo sus IDs.
        df_fact_sm_backlog = df_fact_sm_backlog.drop(
            ['CUSTOMER_ID', 'DEPT_ID', 'DEPT', 'CATEGORY', 'PRIORITY', 'PRIORIDAD',
             "OPEN_GROUP", "ASSIGNED_GROUP", "ServicesManager", "OPENED_BY"], axis=1)

        # Creo la columna "INSERTAR_DT" con la fecha de hoy con la que se insertará al cubo
        today = pd.Timestamp("today").strftime("%Y-%m-%d")
        df_fact_sm_backlog['INSERTAR_DT'] = today

        # Parseo fecha que esta en formato obj a datetime.
        df_fact_sm_backlog['INSERTAR_DT'] = pd.to_datetime(
            df_fact_sm_backlog['INSERTAR_DT'])

        # cumplimiento
        df_sm_backlog = cumplimiento(
            df_fact_sm_backlog, dim_sm_backlog.dim_categoria_backlog)
        # SLA_BACK_CALC
        df_sm_backlog = SLA_BACK_CALC(
            df_fact_sm_backlog, dim_sm_backlog.dim_categoria_backlog)
        # operacion
        df_sm_backlog = operacion(
            df_fact_sm_backlog, dim_sm_backlog.dim_cliente_backlog)

        df_sql = pd.read_sql(conn.select_table_limit_query(
            table='fact_sm_backlog'), conn.conecction_db())

        if not df_sql.empty:
            conn.truncate_table('fact_sm_backlog')

        df_fact_sm_backlog.to_sql(
            'fact_sm_backlog', con=conn.conecction_db(), if_exists='append', index=False)
    else:
        pass


def fact_sm_cerrado():

    df_sql = pd.read_sql(conn.select_table_query(
        '*', 'sm_cerrado'), conn.conecction_db())

    if not df_sql.empty:

        dim_sm_cerrado = DimensionesSMC()
        df_sm_cerrado = df_sql.convert_dtypes()
        df_sm_cerrado = trim_all_columns(df_sm_cerrado)

        df_sm_cerrado_new = pd.DataFrame(
            columns=["TECNOLOGIA", "FECHA_INGRESO_ESTADO_CORTA", "ID_COMPANIA"])

        df_sm_cerrado = df_sm_cerrado.fillna('ND')

        df_sm_backlog = pd.concat([df_sm_cerrado, df_sm_cerrado_new], axis=1)

        df_sm_cerrado = df_sm_backlog

        df_sm_cerrado = df_sm_cerrado.fillna('ND')

        # Hago el merge de los dataframes vs las dimensiones por sus campos que comparten.
        df_fact_sm_cerrado = pd.merge(df_sm_cerrado,
                                      dim_sm_cerrado.dim_categoria, on=["Nueva_Prioridad_Calc", "PRIORIDAD_ATENCION"]).merge(
            dim_sm_cerrado.dim_cliente, on="NOMBRE_CLIENTE").merge(
            dim_sm_cerrado.dim_grupo_sm, on="GRUPO_ASIGNADO").merge(
            dim_sm_cerrado.dim_servicio, on="SERVICIO").merge(
            dim_sm_cerrado.dim_usuario_sm, on=["USUARIO_INSERTADO", "USUARIO_ASIGNADO", "USUARIO_SOLUCION", "USUARIO_CERRADO"]).merge(
            dim_sm_cerrado.dim_atencion, on=["ATENCION", "Reclasificacion_atencion_Calc"]).convert_dtypes()
        # Elimino las columnas que sobran del DF dejando solo sus IDs.
        df_fact_sm_cerrado = df_fact_sm_cerrado.drop(
            ['NOMBRE_CLIENTE', 'Nueva_Prioridad_Calc', 'PRIORIDAD_ATENCION', "ATENCION", "Reclasificacion_atencion_Calc",
             "USUARIO_INSERTADO", "USUARIO_ASIGNADO", "USUARIO_SOLUCION", "USUARIO_CERRADO", 'SERVICIO', "GRUPO_ASIGNADO"], axis=1)

        # Tecnologia
        df_fact_sm_cerrado = tecnologia(df_fact_sm_cerrado)

        # Id_compania
        df_fact_sm_cerrado = id_compania(
            df_fact_sm_cerrado, dim_sm_cerrado.dim_cliente)

        # FECHA_INGRESO_ESTADO_CORTA
        df_fact_sm_cerrado = FECHA_INFGESO_ESTADO_CORTA(df_fact_sm_cerrado)

        # EXCLUYENTE_PROACTIVO
        df_fact_sm_cerrado = EXCLUYENTE_PROACTIVO(
            df_fact_sm_cerrado, dim_sm_cerrado.dim_usuario_sm)

        # Le cambio los tipos para que conicndia el df del stagin vs el de la BD para la hora del merge.
        df_fact_sm_cerrado['DT_CONCILIADO'] = df_fact_sm_cerrado['DT_CONCILIADO'].astype(
            str)
        #df_fact_sm_cerrado['FECHA_SOLUCION'] = df_fact_sm_cerrado['FECHA_SOLUCION'].astype(str)
        df_fact_sm_cerrado['FECHA_INGRESO_ESTADO_CORTA'] = df_fact_sm_cerrado['FECHA_INGRESO_ESTADO_CORTA'].astype(
            str)

        df_fact_sm_cerrado = trim_all_columns(df_fact_sm_cerrado)

        df_fact_sm_cerrado_bd = pd.read_sql(conn.select_table_query(
            column='*', table='fact_sm_cerrado'), conn.conecction_db())

        df_fact_sm_cerrado_bd = df_fact_sm_cerrado_bd.drop(
            ['id_fact_sm_cerrado', 'INSERTAR_DT'], axis=1)

        df_fact_sm_cerrado_bd['FECHA_INGRESO_ESTADO_CORTA'] = df_fact_sm_cerrado_bd['FECHA_INGRESO_ESTADO_CORTA'].astype(
            str)

        df_fact_sm_cerrado_bd = trim_all_columns(df_fact_sm_cerrado_bd)

        # Logica para comparar dos dataframes y encontrar las diferencias que se encuentran solo en el de la izquierda (al stagin area)
        df_merge_left = df_fact_sm_cerrado.merge(
            df_fact_sm_cerrado_bd, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']

        df_merge_left = df_merge_left.drop(['_merge'], axis=1)

        df_merge_left.reset_index(drop=True, inplace=True)

        tiquets_list = df_merge_left['LLAVE_GENERAL_Calc'].tolist()

        for tiquete in tiquets_list:
            conn.delete_llave_unica(table='fact_sm_cerrado', tiquete=tiquete)

        # Creo la columna "INSERTAR_DT" con la fecha de hoy con la que se insertará al cubo
        today = pd.Timestamp("today").strftime("%Y-%m-%d")

        df_fact_sm_cerrado = df_merge_left

        df_fact_sm_cerrado['INSERTAR_DT'] = today

        # Parseo fecha que esta en formato obj a datetime.
        df_fact_sm_cerrado['INSERTAR_DT'] = pd.to_datetime(
            df_fact_sm_cerrado['INSERTAR_DT'])

        df_fact_sm_cerrado['FECHA_SOLUCION'].replace(
            {"ND": pd.NA}, inplace=True)

        df_fact_sm_cerrado.to_sql('fact_sm_cerrado', con=conn.conecction_db(),
                                  if_exists='append', index=False)
    else:
        pass
