import pandas as pd
import numpy as np
from config import Conexion
from dimensiones_clic_abiertos import DimensionesClic

conn = Conexion()


def run():
    df_sql= pd.read_sql(conn.select_table_query('*', 'clic_abiertos'), conn.conecction_db())
    
    #print(df_sql)

    dim_clic_abiertos = DimensionesClic()

    #print(dim_clic_abiertos.dim_usuario)

    #df_clic_abiertos = df_sql.loc[:,['CASO_USUARIO']].convert_dtypes()
    df_clic_abiertos = df_sql.convert_dtypes()

    print(df_clic_abiertos)

    columns = conn.select_columns_table(table='fact_clic_abiertos')

    #print(columns)

    df_fact_clic_abiertos = pd.DataFrame(columns= [column for column in columns])

    #print(df_fact_clic_abiertos)





    
if __name__ == '__main__':
    run()