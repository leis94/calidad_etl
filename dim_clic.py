import pandas as pd
import numpy as np
from config import Conexion

conn = Conexion()

def run():
    

    clic_abiertos_sql = "SELECT * FROM calidad_etl.clic_abiertos;"

    df_sql= pd.read_sql(clic_abiertos_sql, conn.conecction_db())

    df_sql = df_sql.convert_dtypes()

    # Busco los valores unicos (no repeitodos) de las columnas que serán dimensiones, como esto devuelve un np.array lo convierto en un dataframe de nuevo
    df_dim_usuario = pd.DataFrame((df_sql.loc[:,"CASO_USUARIO"].unique()), columns=["CASO_USUARIO"])
    df_dim_prioridad = pd.DataFrame((df_sql.loc[:,"PRIORIDAD"].unique()), columns=["PRIORIDAD"])
    df_dim_categoria = pd.DataFrame((df_sql.loc[:,"CATEGORIA"].unique()), columns=["CATEGORIA"])
    df_dim_estado = pd.DataFrame((df_sql.loc[:,"ESTADO"].unique()), columns=["ESTADO"])
    df_dim_grupo = pd.DataFrame((df_sql.loc[:,"GRUPO"].unique()), columns=["GRUPO"])
    df_dim_asignatario = pd.DataFrame((df_sql.loc[:,"ASIGNATARIO"].unique()), columns=["ASIGNATARIO"])


    df_dim_usuario.dropna(axis=0, inplace=True)
    df_dim_prioridad.dropna(axis=0, inplace=True)
    df_dim_categoria.dropna(axis=0, inplace=True)
    df_dim_estado.dropna(axis=0, inplace=True)
    df_dim_grupo.dropna(axis=0, inplace=True)
    df_dim_asignatario.dropna(axis=0, inplace=True)


    dict_dfs = { "dim_usuario": ['CASO_USUARIO', df_dim_usuario],
                "dim_prioridad": ['PRIORIDAD', df_dim_prioridad],
                "dim_categoria": ['CATEGORIA', df_dim_categoria],
                "dim_estado": ['ESTADO', df_dim_estado],
                "dim_grupo": ['GRUPO', df_dim_grupo],
                "dim_asignatario": ['ASIGNATARIO', df_dim_asignatario]
    }

    # Llamo la función de comparar dimensiones vs valores nuevos provenientes en el excel.
    dataframes = comparar_dimensiones_vs_valores_nuevos(dict_dfs)

    for table, df in dataframes.items():
        
        print(table)
        print(df)
        df.to_sql(f'{table}', con=conn.conecction_db(), if_exists='append', index=False)
        print(f'Inserto en la tabla: {table}')


def comparar_dimensiones_vs_valores_nuevos(dfs):

    dict_dfs = {}

    for table, atributos_and_df in dfs.items():

        #Convierto las columnas en obj de string de pandas, luego convierto los objectos en strings y por ultimo ordeno por la columna para compararlos. 

        df = atributos_and_df[1].applymap(str).convert_dtypes().sort_values(by=atributos_and_df[0])

        #print(df)

        dim_sql = f"SELECT {atributos_and_df[0]} FROM calidad_etl.{table} WHERE id_{table} <> 1;"

        df_dim_bd= pd.read_sql(dim_sql, conn.conecction_db())

        #Convierto los tipo objectos del df en strings y por ultimo ordeno por la columna para compararlos.
        df_dim_bd = df_dim_bd.convert_dtypes().sort_values(by=f'{atributos_and_df[0]}')

        # # Juntos los dos dataframes y elimino los duplicados, para dejar solo las diferencias.
        # df_diff = pd.concat([df,df_dim_bd]).drop_duplicates(keep=False)
        
        #print(df_dim_bd)

        # Logica para comparar dos dataframes y encontrar las diferencias que se encuentran solo en el de la izquierda (al stagin area)
        df_merge_left = df.merge(df_dim_bd,indicator = True, how='left').loc[lambda x : x['_merge']!='both']

        df_merge_left = df_merge_left.iloc[0:,[0]]
        df_merge_left.reset_index(drop=True, inplace=True)
        
        #print(df_merge_left)

        if not df_merge_left.empty:

            dict_dfs[f'{table}'] = df_merge_left
        

    return dict_dfs


if __name__ == '__main__':
    run()
    print("fin del programa")