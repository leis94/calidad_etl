from numpy.lib.function_base import append
import pandas as pd
import numpy as np
from config.config import Conexion

conn = Conexion()

def run():
    
    df_exel_backlog = pd.read_excel(
        r'C:/Users/Elizabeth Cano/OneDrive - ITS InfoCom/Documentos/prueba.xlsx'
        )
    columns_names_sql = """SHOW columns FROM calidad_process.sm_backlog;"""
    df_sql = pd.read_sql(columns_names_sql, conn.conecction_db())
    columns_bd = df_sql.iloc[0:, [0]]
    columns_bd = columns_bd['Field'].tolist()
    # Cambio las columnas del dataframe que están con espacios por _ las cuales son las de la BD.
    df_exel_backlog.columns = df_exel_backlog.columns[:0].tolist(
    ) + columns_bd
    sm_backlog_sql = """SELECT * FROM calidad_process.sm_backlog LIMIT 1;"""
    df_sql = pd.read_sql(sm_backlog_sql, conn.conecction_db())
    if not df_sql.empty:
        conn.truncate_table('sm_backlog')
    
    df_exel_backlog.to_sql(
        'sm_backlog', con=conn.conecction_db(), if_exists='append', index=False)


if __name__ == '__main__':
    run()