import pandas as pd
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
         'STATUS_', 'ESS_ENTRY', 'TITLE', 'BRIEF_DESCRIPTION', 'CAUSE_CODE',
         'CLR_TXT_RESPONSABLE', 'LOGICAL_NAME', 'RESOLUTION_CODE', 'OPENED_BY',
         'OPEN_GROUP', 'ASSIGNEE_NAME', 'ASSIGNED_GROUP', 'RESOLVED_BY',
         'RESOLVED_GROUP', 'CLOSED_BY', 'CLOSED_GROUP', 'CLR_SERVICE_CODE',
         'AFFECTED_ITEM', 'CLOSE_TIME', 'MAYOR_INCIDENT',
         'TIME_HRS', 'RF_FILTRO', 'SLA', 'Cumplimiento', 'Cumplimiento_Gral',
         'RESOLVED_TIME', 'VENDOR', 'REFERENCE_NO', 'DEPEND',
         'Filtro_Relacion', 'INITIAL_STATE', 'FINAL_STATE', 'State', 'SOURCE_',
         'MAJOR_ALARM', 'FCR', 'CALENDAR', 'SLO_ID',
         'SLO_NAME', 'SUBCATEGORY', 'AREA_', 'ECOSISTEMA', 'SEGMENTO', 'CIUDAD',
         'TKMAXIMO', 'KPF_ID', 'KPF_CAUSA', 'ELEMENTO', 'DOWNTIME_START',
         'DOWNTIME_END', 'CONTACT_NAME', 'MASTER_INCIDENT',
         'DOWNTIME_CONCILIADO', 'NUMERO_CUN', 'PRODUCTO', 'DOCTYPE',
         'ORGANIZATIONTYPE', 'ORGANIZATIONTYPE_EYN', 'Es_Duplicados',
         'Form_Rango', 'TECNOLOGIA',
         'Existe_Interacción_SD', 'SIGLA', 'ServicesManager', 'SYSMODTIME',
         'TIPOSERVICIO', 'SUBTIPO', 'REQUESTED_DATE'], axis=1)

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

    sql_fact = pd.read_sql(conn.select_table_date_query(
        column='*', table='fact_sm_backlog', date=today), conn.conecction_db())

    if not sql_fact.empty:
        conn.delete_data(table='fact_sm_backlog', date=today)

    df_fact_sm_backlog.to_sql(
        'fact_sm_backlog', con=conn.conecction_db(), if_exists='append', index=False)


def fact_sm_cerrado():
    df_sql = pd.read_sql(conn.select_table_query(
        '*', 'sm_cerrado'), conn.conecction_db())

    dim_sm_cerrado = DimensionesSMC()
    df_sm_cerrado = df_sql.convert_dtypes()
    df_sm_cerrado = trim_all_columns(df_sm_cerrado)

    df_sm_cerrado_new = pd.DataFrame(
        columns=["TECNOLOGIA", "FECHA_INGRESO_ESTADO_CORTA", "ID_COMPANIA"])

    df_sm_backlog = pd.concat([df_sm_cerrado, df_sm_cerrado_new], axis=1)

    df_sm_cerrado = df_sm_cerrado.fillna('ND')

    # Hago el merge de los dataframes vs las dimensiones por sus campos que comparten.
    df_fact_sm_cerrado = pd.merge(df_sm_cerrado,
                                  dim_sm_cerrado.dim_categoria, on=["ATENCION", "PRIORIDAD_ATENCION"]).merge(
        dim_sm_cerrado.dim_cliente, on="NOMBRE_CLIENTE").merge(
        dim_sm_cerrado.dim_grupo_sm, on="GRUPO_ASIGNADO").merge(
        dim_sm_cerrado.dim_servicio, on="SERVICIO").merge(
        dim_sm_cerrado.dim_usuario_sm, on=["USUARIO_INSERTADO", "USUARIO_ASIGNADO", "USUARIO_SOLUCION", "USUARIO_CERRADO"]).convert_dtypes()
    # Elimino las columnas que sobran del DF dejando solo sus IDs.
    df_fact_sm_cerrado = df_fact_sm_cerrado.drop(
        ['NIT', 'ID_CLIENTE_ONYX', 'NOMBRE_CLIENTE',
         'ATENCION', 'PRIORIDAD_ATENCION', 'ESTADO_ATENCION', 'RESUMEN_',
         'USUARIO_INSERTADO', 'GRUPO_INSERTADO',
         'USUARIO_ASIGNADO', 'GRUPO_ASIGNADO', 'USUARIO_SOLUCION',
         'GRUPO_SOLUCION', 'USUARIO_CERRADO', 'GRUPO_CERRADO',
         'SERVICIO', 'PRODUCTO', 'FAMILIA', 'DT_AJUSTADO', 'MAYOR_INCIDENT',
         'GRUPO_OBJETIVO', 'SEGMENTO', 'PALABRA_CLAVE',
         'NIT_SIN_COD_Calc', 'Clasif_Por_Cliente_Calc', 'Meta_Individual_Calc',
         'Meta_Presidencia_Calc', 'Meta_IFI_Calc', '3G_Calc', 'TMX_Minutos_Calc',
         'TMX_Calc', 'Cluster_Actual_Calc',
         'Celula_Comercial_Calc', 'Clasif_Por_IVR_Calc',
         'DT_CONCILIADO', 'CONTACTO_DESTINO', 'ALIAS_ENLACE',
         'FECHA_SOLUCION', 'TIEMPO_INCIDENTE', 'RANGO_HORA', 'TICKET_MAXIMO',
         'DIRECTOR', 'PN', 'G_SEGMENTO', 'G_Clasif_Por_IVR_Calc',
         'NODO', 'Agrupacion_Especiales', 'Interno_Externo',
         'Numero_CUN', 'TIPOSERVICIO', 'SUBTIPO'], axis=1)

    # Creo la columna "INSERTAR_DT" con la fecha de hoy con la que se insertará al cubo
    today = pd.Timestamp("today").strftime("%Y-%m-%d")
    df_fact_sm_cerrado['INSERTAR_DT'] = today

    # Parseo fecha que esta en formato obj a datetime.
    df_fact_sm_cerrado['INSERTAR_DT'] = pd.to_datetime(
        df_fact_sm_cerrado['INSERTAR_DT'])

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

    df_sql = pd.read_sql(conn.select_table_date_query(
        column='*', table='fact_sm_cerrado', date=today), conn.conecction_db())

    if not df_sql.empty:
        conn.delete_data(table='fact_sm_cerrado', date=today)

    df_fact_sm_cerrado.to_sql('fact_sm_cerrado', con=conn.conecction_db(),
                              if_exists='append', index=False)
