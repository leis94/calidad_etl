import pandas as pd
import numpy as np
from config import Conexion
import pdb

conn = Conexion()

def trim_all_columns(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    def trim_strings(x): return x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)

# Columnas con calculos de calidad para backlog
def prioridad():

    dim_sql_categoria = f"SELECT id_dim_categoria_backlog, PRIORITY, PRIORIDAD FROM calidad_process.dim_categoria_backlog WHERE id_dim_categoria_backlog <> 1;"
    df_dim_bd = pd.read_sql(dim_sql_categoria, conn.conecction_db())
    df_dim_bd = df_dim_bd.convert_dtypes()
    
    df_dim_bd['PRIORIDAD'] = df_dim_bd.PRIORITY.map(lambda x: 'Prioridad '+str(x[0]) if x != 'ND' else 'ND')
    # pdb.set_trace()
    for row in df_dim_bd.iloc:
        conn.conecction_db().execute(f"UPDATE calidad_process.dim_categoria_backlog SET PRIORIDAD = '{row.PRIORIDAD}' WHERE id_dim_categoria_backlog = {row.id_dim_categoria_backlog};")

def cumplimiento(fact,dim):
    fact
    relacion = pd.merge(fact,dim, on='id_dim_categoria_backlog')
    #pdb.set_trace()
    num = [7,10,15]
    num_ = [15,20,30]
    for i in range(0,len(num)):
        fact['CUMPLIMIENTO_CAL'] = relacion.applymap(lambda x: 'Cumple' if x == 'Incident' and x == "Prioridad d%"%i and x <= num[i] else 'No Cumple')
        fact['CUMPLIMIENTO_CAL'] = relacion.applymap(lambda x: 'Cumple' if x == 'Requerimiento' and x == "Prioridad d%"%i and x <= num_[i] else 'No Cumple')
    
    return fact

def SLA_BACK_CALC(fact,dim):
    
    relacion = pd.merge(fact,dim, on="id_dim_categoria_backlog")

    num = [7,10,15]
    num_ = [15,20,30]
    for i in range(0,len(num)):
        fact['SLA_BACK_CALC'] = relacion.applymap(lambda x: num[i] if x == 'Incident' and x == "Prioridad d%"%i else 0)
        fact['SLA_BACK_CALC'] = relacion.applymap(lambda x: num_[i] if x == 'Requerimiento' and x == "Prioridad d%"%i else 0)
    
    sla_valor = fact["SLA_BACK_CALC"]
    form_dias = fact["Form_Dias_BackLog"]

    fact["VENCIMIENTO"] = sla_valor - form_dias

    return fact     

def operacion(fact,dim):

    relacion = pd.merge(fact,dim, on="id_dim_cliente_backlog")
   
    condicion = [
        (relacion["CUSTOMER_ID"] == "890900608-9") & (relacion["CUSTOMER_ID"] == "860003020-1") & 
        (relacion["CUSTOMER_ID"] == "860032330-3") & (relacion["CUSTOMER_ID"] == "800183987-0") & 
        (relacion["CUSTOMER_ID"] == "800240882-0") & (relacion["CUSTOMER_ID"] == "900640173-6")
    ]

    valor = ['ITS']
    fact["OPERACION"] = np.select(condicion,valor,default='ND')

    return fact

# Columnas con calculos de calidad para cerrados
def tecnologia(fact):
    
    fact["TECNOLOGIA"] = np.where(fact["SISTEMA_Calc"]=='RR','HFC','FO')

    return fact

def id_compania(fact,dim):

    relacion = pd.merge(fact,dim, on="id_dim_cliente")
    # pdb.set_trace()
    condiciones = [
        relacion["NOMBRE_CLIENTE"]=='BBVA COLOMBIA',
        relacion["NOMBRE_CLIENTE"]=='ALMACENES EXITO S.A.'
    ]
    value = [1,2]
    fact["ID_COMPANIA"] = np.select(condiciones,value, default=3)
    
    return fact

def FECHA_INFGESO_ESTADO_CORTA(fact):

    fecha = fact["FECHA_INGRESO_ESTADO"]
    fact["FECHA_INGRESO_ESTADO_CORTA"] = fecha
    # pdb.set_trace()

    return fact

def EXCLUYENTE_PROACTIVO(fact,dim):

    df_sql = pd.read_sql(conn.select_table_query(
        '*', 'dim_usuarios_insertado'), conn.conecction_db())
    df_dim_usuarios_insertado = df_sql.convert_dtypes()
    df_dim_usuarios_insertado  = trim_all_columns(df_dim_usuarios_insertado)

    relacion = pd.merge(dim,df_dim_usuarios_insertado,on="USUARIO_INSERTADO")
    relacion = relacion.drop(["USUARIO_INSERTADO","USUARIO_ASIGNADO","USUARIO_SOLUCION","USUARIO_CERRADO","OPERACIÓN"],axis=1)
    
    if not relacion.empty:
        fact = pd.merge(fact,relacion,on="id_dim_usuario_sm")
    else: fact['EXCLUYENTE_PROACTIVO'] = 'Excluir'

    fact['EXCLUYENTE_PROACTIVO'].fillna('Excluir')
    pdb.set_trace()
    return fact