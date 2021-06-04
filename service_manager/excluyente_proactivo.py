import pandas as pd
from pandas.core.frame import DataFrame
from config import Conexion
import pdb

conn = Conexion()

def run():

    df_exel_backlog = pd.read_excel(
        r'C:/Users/Elizabeth Cano/OneDrive - ITS InfoCom/Documentos/excluyente_proactivo.xlsx'
        )
    columns_names_sql = """SHOW columns FROM calidad_process.dim_usuarios_insertado;"""
    df_sql = pd.read_sql(columns_names_sql, conn.conecction_db())
    columns_bd = df_sql.iloc[0:, [0]]
    columns_bd = columns_bd['Field'].tolist()
    pdb.set_trace()
    # Cambio las columnas del dataframe que están con espacios por _ las cuales son las de la BD.
    df_exel_backlog.columns = df_exel_backlog.columns[:0].tolist(
    ) + columns_bd
    sm_backlog_sql = """SELECT * FROM calidad_process.dim_usuarios_insertado LIMIT 1;"""
    df_sql = pd.read_sql(sm_backlog_sql, conn.conecction_db())

    if not df_sql.empty:
        conn.truncate_table('dim_usuarios_insertado')
    pdb.set_trace()
    df_exel_backlog.to_sql(
        'dim_usuarios_insertado', con=conn.conecction_db(), if_exists='append', index=False)
    
if __name__ == '__main__':
    run()