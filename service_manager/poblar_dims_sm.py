import pandas as pd
from config.config import Conexion

conn = Conexion()

def dimensiones_sm_cerrado():

    sm_cerrado_sql = "SELECT * FROM calidad_process.sm_cerrado;"
    df_sql = pd.read_sql(sm_cerrado_sql, conn.conecction_db())
    df_sql = df_sql.convert_dtypes()

    # Busco los valores unicos (no repeitodos) de las columnas que serán dimensiones, como esto devuelve un np.array lo convierto en un dataframe de nuevo
    dim_categoria = pd.DataFrame(
        (df_sql.loc[:, ["ATENCION","PRIORIDAD_ATENCION"]].drop_duplicates()), columns=["ATENCION","PRIORIDAD_ATENCION"])
    dim_cliente = pd.DataFrame(
        (df_sql.loc[:, "NOMBRE_CLIENTE"].unique()), columns=["NOMBRE_CLIENTE"])
    dim_grupo_sm = pd.DataFrame(
        (df_sql.loc[:, "GRUPO_ASIGNADO"].unique()), columns=["GRUPO_ASIGNADO"])
    dim_servicio = pd.DataFrame(
        (df_sql.loc[:, "SERVICIO"].unique()), columns=["SERVICIO"])
    dim_usuario_sm = pd.DataFrame(
        (df_sql.loc[:, ["USUARIO_INSERTADO","USUARIO_ASIGNADO","USUARIO_SOLUCION","USUARIO_CERRADO"]].drop_duplicates()), columns=["USUARIO_INSERTADO","USUARIO_ASIGNADO","USUARIO_SOLUCION","USUARIO_CERRADO"])

    dim_categoria = dim_categoria.fillna('ND')
    dim_cliente = dim_cliente.fillna('ND')
    dim_grupo_sm = dim_grupo_sm.fillna('ND')
    dim_servicio = dim_servicio.fillna('ND')
    dim_usuario_sm = dim_usuario_sm.fillna('ND')

    dict_dfs = {
                "dim_cliente": ["NOMBRE_CLIENTE", dim_cliente],
                "dim_grupo_sm": ["GRUPO_ASIGNADO", dim_grupo_sm],
                "dim_usuario_sm": ["USUARIO_INSERTADO","USUARIO_ASIGNADO","USUARIO_SOLUCION","USUARIO_CERRADO", dim_usuario_sm],
                "dim_categoria": ["ATENCION","PRIORIDAD_ATENCION", dim_categoria],
                "dim_servicio": ["SERVICIO", dim_servicio]
                    }

    # Llamo la función de comparar dimensiones vs valores nuevos provenientes en el excel.

    dataframes_dim = comparar_dimensiones_vs_valores_nuevos_smc(dict_dfs)
    # print(dataframes_dim)
    for table, df in dataframes_dim.items():

        print(table)
        print(df)
        #pdb.set_trace()
        df.to_sql(f'{table}', con=conn.conecction_db(),
                  if_exists='append', index=False)
        print(f'Inserto en la tabla: {table}')

def dimensiones_sm_backlog():

    sm_backlog_sql = "SELECT * FROM calidad_process.sm_backlog;"
    df_sql = pd.read_sql(sm_backlog_sql, conn.conecction_db())
    df_sql = df_sql.convert_dtypes()
    # Busco los valores unicos (no repeitodos) de las columnas que serán dimensiones, como esto devuelve un np.array lo convierto en un dataframe de nuevo
    dim_categoria_backlog = pd.DataFrame(
        (df_sql.loc[:, "PRIORITY"].unique()), columns=["PRIORITY"])
    dim_cliente_backlog = pd.DataFrame(
        (df_sql.loc[:, ["CUSTOMER_ID","DEPT_ID","DEPT"]].drop_duplicates()), columns=["CUSTOMER_ID","DEPT_ID","DEPT"])
    dim_grupo_sm_backlog = pd.DataFrame(
        (df_sql.loc[:, ["OPEN_GROUP","ASSIGNED_GROUP"]].drop_duplicates()), columns=["OPEN_GROUP","ASSIGNED_GROUP"])
    dim_servicio_backlog = pd.DataFrame(
        (df_sql.loc[:, "ServicesManager"].unique()), columns=["ServicesManager"])
    dim_usuarios_sm_backlog = pd.DataFrame(
        (df_sql.loc[:, "OPENED_BY"].unique()), columns=["OPENED_BY"])


    dim_categoria_backlog = dim_categoria_backlog.fillna('ND')
    dim_cliente_backlog = dim_cliente_backlog.fillna('ND')
    dim_grupo_sm_backlog = dim_grupo_sm_backlog.fillna('ND')
    dim_servicio_backlog = dim_servicio_backlog.fillna('ND')
    dim_usuarios_sm_backlog = dim_usuarios_sm_backlog.fillna('ND')
    

    dict_dfs = {
                "dim_cliente_backlog": ["CUSTOMER_ID","DEPT_ID","DEPT", dim_cliente_backlog],
                "dim_grupo_sm_backlog": ["OPEN_GROUP","ASSIGNED_GROUP", dim_grupo_sm_backlog],
                "dim_usuarios_sm_backlog": ["OPENED_BY", dim_usuarios_sm_backlog],
                "dim_categoria_backlog": ["PRIORITY", dim_categoria_backlog],
                "dim_servicio_backlog": ["ServicesManager", dim_servicio_backlog]
                    }
    # Llamo la función de comparar dimensiones vs valores nuevos provenientes en el excel.
    dataframes_dim = comparar_dimensiones_vs_valores_nuevos_smb(dict_dfs)
    # pdb.set_trace()
    for table, df in dataframes_dim.items():

        print(table)
        print(df)
        #pdb.set_trace()
        df.to_sql(f'{table}', con=conn.conecction_db(),
                  if_exists='append', index=False)
        print(f'Inserto en la tabla: {table}')

def trim_all_columns(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)

def comparar_dimensiones_vs_valores_nuevos_smc(dfs):

    dict_dfs = {}

    for table, atributos_and_df in dfs.items():
        
        if len(atributos_and_df) == 2:
            print('una dimension')
            # Convierto las columnas en obj de string de pandas, luego convierto los objectos en strings y por ultimo ordeno por la columna para compararlos.
            df = atributos_and_df[1].applymap(
                str).convert_dtypes().sort_values(by=atributos_and_df[0])
            # Llamar a la función para hacer str a los parametros strings del dataframe
            df = trim_all_columns(df)
            dim_sql = f"SELECT {atributos_and_df[0]} FROM calidad_process.{table} WHERE id_{table} <> 1;"
            df_dim_bd = pd.read_sql(dim_sql, conn.conecction_db())
            # Convierto los tipo objectos del df en strings y por ultimo ordeno por la columna para compararlos.
            df_dim_bd = df_dim_bd.convert_dtypes().sort_values(
                by=f'{atributos_and_df[0]}')
            # Llamar a la función para hacer str a los parametros strings del dataframe
            df_dim_bd = trim_all_columns(df_dim_bd)
            # Logica para comparar dos dataframes y encontrar las diferencias que se encuentran solo en el de la izquierda (al stagin area)
            

            # print('excel')
            # print(df)
            # print('sql')
            # print(df_dim_bd)

            df_merge_left_one = df.merge(
                df_dim_bd, indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']
            df_merge_left_one = df_merge_left_one.drop(['_merge'],axis=1)
            # df_merge_left_one = df_merge_left_one.iloc[0:, [0]]
            df_merge_left_one.reset_index(drop=True, inplace=True)

            # pdb.set_trace()

            if not df_merge_left_one.empty:
                dict_dfs[f'{table}'] = df_merge_left_one
        
        elif len(atributos_and_df) == 3:
            print('dos dimension')
            # Convierto las columnas en obj de string de pandas, luego convierto los objectos en strings y por ultimo ordeno por la columna para compararlos.
            df__ = atributos_and_df[2].applymap(
                str).convert_dtypes().sort_values(by=atributos_and_df[0])
            # Llamar a la función para hacer str a los parametros strings del dataframe
            df__ = trim_all_columns(df__)
            dim_sql_ = f"SELECT {atributos_and_df[0]},{atributos_and_df[1]} FROM calidad_process.{table} WHERE id_{table} <> 1;"
            df_dim_bd_ = pd.read_sql(dim_sql_, conn.conecction_db())
            # Convierto los tipo objectos del df_ en strings y por ultimo ordeno por la columna para compararlos.
            df_dim_bd_ = df_dim_bd_.convert_dtypes().sort_values(
                by=f'{atributos_and_df[0]}')
            # Llamar a la función para hacer str a los parametros strings del dataframe
            df_dim_bd_ = trim_all_columns(df_dim_bd_)
            # Logica para comparar dos dataframes y encontrar las diferencias que se encuentran solo en el de la izquierda (al stagin area)
            
            # print('excel')
            # print(df__)
            # print('sql')
            # print(df_dim_bd_)

            df_merge_left_two = df__.merge(
                df_dim_bd_, indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']
            df_merge_left_two = df_merge_left_two.drop(['_merge'],axis=1)
            # df_merge_left_two = df_merge_left_two.iloc[0:, [0,1]]
            df_merge_left_two.reset_index(drop=True, inplace=True)

            if not df_merge_left_two.empty:
                dict_dfs[f'{table}'] = df_merge_left_two

        elif len(atributos_and_df) == 5:
            print('cuatro dimension')
            # Convierto las columnas en obj de string de pandas, luego convierto los objectos en strings y por ultimo ordeno por la columna para compararlos.
            df__ = atributos_and_df[4].applymap(
                str).convert_dtypes().sort_values(by=atributos_and_df[0])
            # Llamar a la función para hacer str a los parametros strings del dataframe
            df__ = trim_all_columns(df__)
            dim_sql__= f"SELECT {atributos_and_df[0]},{atributos_and_df[1]},{atributos_and_df[2]},{atributos_and_df[3]} FROM calidad_process.{table} WHERE id_{table} <> 1;"
            df_dim_bd__ = pd.read_sql(dim_sql__, conn.conecction_db())
            # Convierto los tipo objectos del df__ en strings y por ultimo ordeno por la columna para compararlos.
            df_dim_bd__ = df_dim_bd__.convert_dtypes().sort_values(
                by=f'{atributos_and_df[0]}')
            # Llamar a la función para hacer str a los parametros strings del dataframe
            df_dim_bd__ = trim_all_columns(df_dim_bd__)
            # Logica para comparar dos dataframes y encontrar las diferencias que se encuentran solo en el de la izquierda (al stagin area)
            df_merge_left_three = df__.merge(
                df_dim_bd__, indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']
            
            # print(df_merge_left_three)
            #pdb.set_trace()
            df_merge_left_three = df_merge_left_three.drop(['_merge'],axis=1)
            #df_merge_left_three = df_merge_left_three.iloc[0:, [0,1,2,3,4]]
            df_merge_left_three.reset_index(drop=True, inplace=True)

            if not df_merge_left_three.empty:
                dict_dfs[f'{table}'] = df_merge_left_three

    return dict_dfs

def comparar_dimensiones_vs_valores_nuevos_smb(dfs):

    dict_dfs = {}

    for table, atributos_and_df in dfs.items():

        if len(atributos_and_df) == 2:
            print('una dimension')
            # Convierto las columnas en obj de string de pandas, luego convierto los objectos en strings y por ultimo ordeno por la columna para compararlos.
            df = atributos_and_df[1].applymap(
                str).convert_dtypes()
            # Llamar a la función para hacer str a los parametros strings del dataframe
            df = trim_all_columns(df)
            dim_sql = f"SELECT {atributos_and_df[0]} FROM calidad_process.{table} WHERE id_{table} <> 1;"
            df_dim_bd = pd.read_sql(dim_sql, conn.conecction_db())
            # Convierto los tipo objectos del df en strings y por ultimo ordeno por la columna para compararlos.
            df_dim_bd = df_dim_bd.convert_dtypes()
            # Llamar a la función para hacer str a los parametros strings del dataframe
            df_dim_bd = trim_all_columns(df_dim_bd)
            # Logica para comparar dos dataframes y encontrar las diferencias que se encuentran solo en el de la izquierda (al stagin area)

            df_merge_left_one = df.merge(
                df_dim_bd, indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']
            df_merge_left_one = df_merge_left_one.drop(['_merge'],axis=1)
            df_merge_left_one.reset_index(drop=True, inplace=True)

            if not df_merge_left_one.empty:
                dict_dfs[f'{table}'] = df_merge_left_one
        
        elif len(atributos_and_df) == 3:
            print('dos dimension')

            print(table)
            
            # Convierto las columnas en obj de string de pandas, luego convierto los objectos en strings y por ultimo ordeno por la columna para compararlos.
            df_ = atributos_and_df[2].applymap(
                str).convert_dtypes().sort_values(by=atributos_and_df[0])
            # Llamar a la función para hacer str a los parametros strings del dataframe
            df_ = trim_all_columns(df_)
            dim_sql_ = f"SELECT {atributos_and_df[0]},{atributos_and_df[1]} FROM calidad_process.{table} WHERE id_{table} <> 1;"
            df_dim_bd_ = pd.read_sql(dim_sql_, conn.conecction_db())
            # Convierto los tipo objectos del df_ en strings y por ultimo ordeno por la columna para compararlos.
            df_dim_bd_ = df_dim_bd_.convert_dtypes()
            # Llamar a la función para hacer str a los parametros strings del dataframe
            df_dim_bd_ = trim_all_columns(df_dim_bd_)
            # pdb.set_trace()

            df_merge_left_two = df_.merge(
                df_dim_bd_, indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']
            df_merge_left_two = df_merge_left_two.drop(['_merge'],axis=1)
            df_merge_left_two.reset_index(drop=True, inplace=True)
            # pdb.set_trace()
            if not df_merge_left_two.empty:
                dict_dfs[f'{table}'] = df_merge_left_two
                print(dict_dfs)
        
        elif len(atributos_and_df) == 4:
            print('tres dimension')
            print(table)
            # Convierto las columnas en obj de string de pandas, luego convierto los objectos en strings y por ultimo ordeno por la columna para compararlos.
            df__ = atributos_and_df[3].applymap(
                str).convert_dtypes().sort_values(by=atributos_and_df[0])
            # Llamar a la función para hacer str a los parametros strings del dataframe
            df__ = trim_all_columns(df__)
            dim_sql_ = f"SELECT {atributos_and_df[0]},{atributos_and_df[1]},{atributos_and_df[2]} FROM calidad_process.{table} WHERE id_{table} <> 1;"
            df_dim_bd_ = pd.read_sql(dim_sql_, conn.conecction_db())
            # Convierto los tipo objectos del df__ en strings y por ultimo ordeno por la columna para compararlos.
            df_dim_bd_ = df_dim_bd_.convert_dtypes()
            # Llamar a la función para hacer str a los parametros strings del dataframe
            df_dim_bd_ = trim_all_columns(df_dim_bd_)
            # Logica para comparar dos dataframes y encontrar las diferencias que se encuentran solo en el de la izquierda (al stagin area)

            df_merge_left_t = df__.merge(
                df_dim_bd_, indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']
            df_merge_left_t = df_merge_left_t.drop(['_merge'],axis=1)
            df_merge_left_t.reset_index(drop=True, inplace=True)

            if not df_merge_left_t.empty:
                dict_dfs[f'{table}'] = df_merge_left_t
            
    return dict_dfs

if __name__ == '__main__':
    #dimensiones_sm_cerrado()
    dimensiones_sm_backlog()
    print("fin del programa")
