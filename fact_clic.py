import pandas as pd
import numpy as np
from config import Conexion
from dimensiones_clic import DimensionesClic

conn = Conexion()


def run():
    df_sql = pd.read_sql(conn.select_table_query(
        '*', 'clic_abiertos'), conn.conecction_db())

    dim_clic_abiertos = DimensionesClic()

    df_clic_abiertos = df_sql.convert_dtypes().sort_values(by='FECHA_DE_APERTURA')

    df_clic_abiertos = df_clic_abiertos.fillna('ND')

    # Elimino el valor ND dado que debo convertir int el df para compararlo.
    dim_usuario = dim_clic_abiertos.dim_usuario
    dim_usuario = dim_usuario[dim_usuario.CASO_USUARIO != 'ND'].astype('int64')

    # Hago el merge de los dataframes vs las dimensiones por sus campos que comparten.
    df_fact_clic_abiertos = pd.merge(df_clic_abiertos, dim_usuario, on='CASO_USUARIO').merge(dim_clic_abiertos.dim_prioridad, on='PRIORIDAD').merge(
        dim_clic_abiertos.dim_categoria, on='CATEGORIA').merge(dim_clic_abiertos.dim_estado, on='ESTADO').merge(dim_clic_abiertos.dim_grupo, on='GRUPO').merge(
            dim_clic_abiertos.dim_asignatario, on='ASIGNATARIO').convert_dtypes()

    # Elimino las columnas que sobran del DF dejando solo sus IDs.
    df_fact_clic_abiertos = df_fact_clic_abiertos.drop(
        ['ES_PADRE', 'CASO_USUARIO', 'PRIORIDAD', 'CATEGORIA', 'ESTADO', 'GRUPO', 'ASIGNATARIO'], axis=1)

    # Creo la columna "INSERTAR_DT" con la fecha de hoy con la que se insertará al cubo
    today = pd.Timestamp("today").strftime("%Y-%m-%d")
    df_fact_clic_abiertos['INSERTAR_DT'] = today

    # Parseo fecha que esta en formato obj a datetime.
    df_fact_clic_abiertos['INSERTAR_DT'] = pd.to_datetime(
        df_fact_clic_abiertos['INSERTAR_DT'])

    df_fact_clic_abiertos.rename(
        columns={"INCIDENTE_NUM": 'TIQUETE'}, inplace=True)

    df_sql = pd.read_sql(conn.select_table_date_query(
        column='*', table='fact_clic_abiertos', date=today), conn.conecction_db())

    if not df_sql.empty:
        conn.delete_data(table='fact_clic_abiertos', date=today)

    df_fact_clic_abiertos.to_sql('fact_clic_abiertos', con=conn.conecction_db(),
                                 if_exists='append', index=False)


if __name__ == '__main__':
    run()
    print("Finalizó el programa")
