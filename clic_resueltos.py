from numpy.lib.function_base import append
import pandas as pd
import numpy as np
import sqlalchemy as sql

database_type = 'mysql'
user = 'root'
password = 'admin123#'
host = 'localhost:3306'
database = 'calidad_etl'

conn_string = '{}://{}:{}@{}/{}?charset=utf8'.format(
                database_type, user, password, host, database,)

sql_conn = sql.create_engine(conn_string)

df_excel = pd.read_excel(r'C:\Users\Cristian Silva\Documents\Repositorios\etl\data\CLIC RESUELTOS 26-04-2021.xlsx')

#Agregando las columnas al df que hacen falta de la BD de CLIC
# df_excel.insert(1, 'CASO_USUARIO', np.nan)
# df_excel.insert(51, 'PROBLEM', np.nan)
# df_excel.insert(52, 'PARENT', np.nan)
# df_excel.insert(53, 'CAUSED_BY_CHANGE_ORDER', np.nan)
# df_excel.insert(54, 'ETIQUETA', np.nan)


#Convertir los formatos object en formatos strings
df_excel = df_excel.convert_dtypes()


columns_names_sql = """
SHOW columns FROM calidad_etl.clic_resueltos;
"""
df_sql= pd.read_sql(columns_names_sql, sql_conn)

columns_clic = df_sql.iloc[0:,[0]]

columns_clic = columns_clic['Field'].tolist()

columnas = df_excel.columns


#Cambio las columnas del dataframe que están con espacios por _ las cuales son las de la BD.
df_excel.columns = df_excel.columns[:0].tolist() + columns_clic

df_excel.to_sql('clic_resueltos', con=sql_conn, if_exists='append', index=False)






