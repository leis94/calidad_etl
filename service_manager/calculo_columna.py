import pandas as pd
import numpy as np
from config.config import Conexion
from config.utils import trim_all_columns


conn = Conexion()


# Columnas con calculos de calidad para backlog
def prioridad():

    dim_sql_categoria = f"SELECT id_dim_categoria_backlog, PRIORITY, PRIORIDAD FROM calidad_etl.dim_categoria_backlog WHERE id_dim_categoria_backlog <> 1;"
    df_dim_bd = pd.read_sql(dim_sql_categoria, conn.conecction_db())
    df_dim_bd = df_dim_bd.convert_dtypes()

    df_dim_bd['PRIORIDAD'] = df_dim_bd.PRIORITY.map(
        lambda x: 'Prioridad '+str(x[0]) if x != 'ND' else 'ND')
    for row in df_dim_bd.iloc:
        conn.conecction_db().execute(
            f"UPDATE calidad_etl.dim_categoria_backlog SET PRIORIDAD = '{row.PRIORIDAD}' WHERE id_dim_categoria_backlog = '{row.id_dim_categoria_backlog}';")


def cumplimiento(fact, dim):
    fact
    relacion = pd.merge(fact, dim, on='id_dim_categoria_backlog')
    # import pdb
    # pdb.set_trace()
    num = [10, 15, 20]
    num_ = [20, 30, 40]
    for i in range(0, len(num)):

        fact['CUMPLIMIENTO_CAL'] = relacion.apply(
            lambda x: 'Cumple' if ((x['New_Category'] == 'Incident' and x['PRIORIDAD'] == 'Prioridad '+str(i+1) and x['Form_Dias_BackLog'] <= num[i]) or
                                   (x['New_Category'] == 'Requerimiento' and x['PRIORIDAD'] == 'Prioridad '+str(i+1) and x['Form_Dias_BackLog'] <= num_[i]) or
                                   (x['New_Category'] == 'Cambios' and x['PRIORIDAD'] == 'Prioridad '+str(i+1) and x['Form_Dias_BackLog'] <= num_[i])) else 'No Cumple', axis=1)
        # fact['CUMPLIMIENTO_CAL'] = relacion.apply(
        #     lambda x: 'Cumple' if (x['New_Category']=='Requerimiento' and x['PRIORIDAD'] == 'Prioridad '+str(i+1) and x['Form_Dias_BackLog'] <= num_[i]) else 'No Cumple', axis=1)
        # fact['CUMPLIMIENTO_CAL'] = relacion.apply(
        #     lambda x: 'Cumple' if (x['New_Category']=='Cambios' and x['PRIORIDAD'] == 'Prioridad '+str(i+1) and x['Form_Dias_BackLog'] <= num_[i]) else 'No Cumple', axis=1)

    return fact


def SLA_BACK_CALC(fact, dim):

    relacion = pd.merge(fact, dim, on="id_dim_categoria_backlog")

    num = [7, 10, 15]
    num_ = [15, 20, 30]
    for i in range(0, len(num)):
        fact['SLA_BACK_CALC'] = relacion.apply(
            lambda x: num[i] if (x['New_Category'] == 'Incident' and x['PRIORIDAD'] == 'Prioridad '+str(i+1) or
                                 (x['New_Category'] == 'Requerimiento' and x['PRIORIDAD'] == 'Prioridad '+str(i+1))) else 0, axis=1)
        # fact['SLA_BACK_CALC'] = relacion.apply(
        #     lambda x: num_[i] if x['New_Category']=='Requerimiento' and x['PRIORIDAD'] == 'Prioridad '+str(i+1) else 0, axis=1)

    sla_valor = fact["SLA_BACK_CALC"]
    form_dias = fact["Form_Dias_BackLog"]

    fact["VENCIMIENTO"] = sla_valor - form_dias

    return fact


def operacion(fact, dim):

    relacion = pd.merge(fact, dim, on="id_dim_cliente_backlog")

    condicion = [
        (relacion["CUSTOMER_ID"] == "890900608-9") & (relacion["CUSTOMER_ID"] == "860003020-1") &
        (relacion["CUSTOMER_ID"] == "860032330-3") & (relacion["CUSTOMER_ID"] == "800183987-0") &
        (relacion["CUSTOMER_ID"] ==
         "800240882-0") & (relacion["CUSTOMER_ID"] == "900640173-6")
    ]

    valor = ['ITS']
    fact["OPERACION"] = np.select(condicion, valor, default='ND')

    return fact

# Columnas con calculos de calidad para cerrados


def tecnologia(fact):

    fact["TECNOLOGIA"] = np.where(fact["SISTEMA_Calc"] == 'RR', 'HFC', 'FO')

    return fact


def id_compania(fact, dim):

    relacion = pd.merge(fact, dim, on="id_dim_cliente")
    condiciones = [
        relacion["NOMBRE_CLIENTE"] == 'BBVA COLOMBIA',
        relacion["NOMBRE_CLIENTE"] == 'ALMACENES EXITO S.A.'
    ]
    value = [1, 2]
    fact["ID_COMPANIA"] = np.select(condiciones, value, default=3)

    return fact


def FECHA_INFGESO_ESTADO_CORTA(fact):

    fecha = fact["FECHA_INGRESO_ESTADO"]
    fact["FECHA_INGRESO_ESTADO_CORTA"] = fecha

    return fact


def EXCLUYENTE_PROACTIVO(fact, dim):

    df_sql = pd.read_sql(conn.select_table_query(
        '*', 'dim_usuarios_insertado'), conn.conecction_db())
    df_dim_usuarios_insertado = df_sql.convert_dtypes()
    df_dim_usuarios_insertado = trim_all_columns(df_dim_usuarios_insertado)

    relacion = pd.merge(dim, df_dim_usuarios_insertado,
                        how="left", on="USUARIO_INSERTADO")
    relacion = relacion.drop(["USUARIO_INSERTADO", "USUARIO_ASIGNADO",
                             "USUARIO_SOLUCION", "USUARIO_CERRADO", "OPERACIÓN"], axis=1)

    if not relacion.empty:
        fact = pd.merge(fact, relacion, on="id_dim_usuario_sm")
    else:
        fact['EXCLUYENTE_PROACTIVO'] = 'Excluir'

    fact['EXCLUYENTE_PROACTIVO'] = fact['EXCLUYENTE_PROACTIVO'].fillna(
        'Excluir')

    return fact
