import pandas as pd
import numpy as np
from pandas.core.arrays.integer import Int64Dtype
from pandas.core.indexes.numeric import Float64Index
from config.config import Conexion
from config.utils import trim_all_columns
from .dimensiones_avaya import DimensionesAvaya

conn = Conexion()


def fact_av_llamadas():
    dims_avaya = DimensionesAvaya()
    df_av_llamadas = pd.read_sql(conn.select_table_query(
        '*', 'av_llamadas'), conn.conecction_db())

    if not df_av_llamadas.empty:

        df_av_llamadas = df_av_llamadas.convert_dtypes()

        # Hago el merge de los dataframes vs las dimensiones por sus campos que comparten.
        df_fact_av_llamadas = pd.merge(df_av_llamadas, dims_avaya.dim_skill, on=['NUMERO_SKILL', 'NOMBRE_SKILL']).merge(
            dims_avaya.dim_intervalo, on=['INICIO_INTERVALO', 'FIN_INTERVALO'])

        df_fact_av_llamadas['PRODUCTO_PARA_TMO'] = df_fact_av_llamadas['LLAMADAS_ATENDIDAS'].fillna(0) * \
            df_fact_av_llamadas['TIEMPO_PROM_ACD'].fillna(0)

        df_fact_av_llamadas['TIEMPO_LLAMADAS_SALIENTES'] = df_fact_av_llamadas['LLAMADAS_SALIENTES_EXTENSION'].fillna(0) * \
            df_fact_av_llamadas['TIEMPO_PROM_CONVER_LLAMADAS_SALIENTES_EXTENSION'].fillna(
                0)

        convert_dict = {
            'NUMERO_SKILL':   np.int64,
            'LLAMADAS_OFRECIDAS':   np.int64,
            'LLAMADAS_ATENDIDAS':   np.int64,
            'LLAMADAS_ABANDONADAS':   np.int64,
            'LLAMADAS_DESB_A_OTRA_COLA':   np.int64,
            'LLAMADAS_DESB_DE_OTRA_COLA':   np.int64,
            'LLAMADAS_DESB_A_OTRA_COLA_SEGUNDA_VEZ':   np.int64,
            'LLAMADAS_TRANSFERIDAS':   np.int64,
            'LLAMADAS_SALIENTES_EXTENSION':   np.int64,
            'LLAMADAS_SALIENTES_EXTERNAS':   np.int64,
            'LLAMADAS_CON_TIEMPOS_HOLD':   np.int64,
            'LLAMADAS_PUESTAS_EN_CONFERENCIA':   np.int64,
            'NUMERO_MAX_LLAMADAS_EN_COLA':   np.int64,
            'DEMORA_MAX_EN_COLA':   np.int64,
            'LLAMADAS_POR_AGENTE':   np.int64,
            'LLAMADAS_ACD_00-05':   np.int64,
            'LLAMADAS_ACD_05-10':   np.int64,
            'LLAMADAS_ACD_10-15':   np.int64,
            'LLAMADAS_ACD_15-20':   np.int64,
            'LLAMADAS_ACD_20-30':   np.int64,
            'LLAMADAS_ACD_30-40':   np.int64,
            'LLAMADAS_ACD_40-50':   np.int64,
            'LLAMADAS_ACD_50-60':   np.int64,
            'LLAMADAS_ACD_60-120':   np.int64,
            'LLAMADAS_ACD_>_120':   np.int64,
            'LLAMADAS_ABAN_00-05':   np.int64,
            'LLAMADAS_ABAN_05-10':   np.int64,
            'LLAMADAS_ABAN_10-15':   np.int64,
            'LLAMADAS_ABAN_15-20':   np.int64,
            'LLAMADAS_ABAN_20-30':   np.int64,
            'LLAMADAS_ABAN_30-40':   np.int64,
            'LLAMADAS_ABAN_40-50':   np.int64,
            'LLAMADAS_ABAN_50-60':   np.int64,
            'LLAMADAS_ABAN_60-120':   np.int64,
            'LLAMADAS_ABAN_>_120':   np.int64,
        }
        # Convierto el tipo de dato Int64 a int64
        for field, new_type in convert_dict.items():
            df_fact_av_llamadas[field] = df_fact_av_llamadas[field].fillna(
                np.nan)
            df_fact_av_llamadas[field] = df_fact_av_llamadas[field].astype(
                new_type)

        df_fact_av_llamadas['ANTES_20'] = df_fact_av_llamadas['LLAMADAS_ACD_00-05'].fillna(0) + df_fact_av_llamadas['LLAMADAS_ACD_05-10'].fillna(0) + \
            df_fact_av_llamadas['LLAMADAS_ACD_10-15'].fillna(0) + \
            df_fact_av_llamadas['LLAMADAS_ACD_15-20'].fillna(0) + \
            df_fact_av_llamadas['LLAMADAS_ACD_20-30'].fillna(0)

        columns_bd = conn.select_columns_table('fact_av_llamadas')
        columns_bd.pop(0)

        columns_df = (df_fact_av_llamadas.columns).to_list()

        # Elimino las columnas que sobran del DF dejando solo las columnas necesarias para insertar en la BD, esto se hace con list comprenhensions.
        df_fact_av_llamadas = df_fact_av_llamadas.drop(
            [columna for columna in columns_df if columna not in columns_bd], axis=1)

        fecha_list = df_fact_av_llamadas['FECHA'].tolist()
        id_dim_skill = df_fact_av_llamadas['ID_DIM_SKILL'].tolist()
        id_dim_intervalo = df_fact_av_llamadas['ID_DIM_INTERVALO'].tolist()

        # For loop para traer los registros existentes ne la base de datos de acuerdo a los que se cargaron en la de staging.
        df_fact_av_llamadas_bd = pd.DataFrame()

        for i in range(len(fecha_list)):

            data_bd = pd.read_sql(conn.select_table_query_existent(
                fecha=fecha_list[i], id_dim_skill=id_dim_skill[i], id_dim_intervalo=id_dim_intervalo[i], table='fact_av_llamadas'), conn.conecction_db())
            df_fact_av_llamadas_bd = pd.concat(
                [df_fact_av_llamadas_bd, data_bd], ignore_index=True)

        df_fact_av_llamadas_bd = df_fact_av_llamadas_bd.drop(
            ['ID_FACT_AV_LLAMADAS', 'INSERTAR_DT'], axis=1)

        # Cambiar el formato a las columnas del df de la bd de la fact vs el df a guardar en la fact para que pueda hacer el merge e encontrar las diferencias si existen o no
        for x in df_fact_av_llamadas.columns:

            if x == 'PRODUCTO_PARA_TMO':

                df_fact_av_llamadas_bd['PRODUCTO_PARA_TMO'] = df_fact_av_llamadas_bd.PRODUCTO_PARA_TMO.astype(
                    np.float64)
                df_fact_av_llamadas['PRODUCTO_PARA_TMO'] = df_fact_av_llamadas.PRODUCTO_PARA_TMO.astype(
                    np.float64)

                df_fact_av_llamadas_bd['TIEMPO_LLAMADAS_SALIENTES'] = df_fact_av_llamadas_bd.TIEMPO_LLAMADAS_SALIENTES.astype(
                    np.float64)
                df_fact_av_llamadas['TIEMPO_LLAMADAS_SALIENTES'] = df_fact_av_llamadas.TIEMPO_LLAMADAS_SALIENTES.astype(
                    np.float64)
            elif x == 'ANTES_20':

                df_fact_av_llamadas_bd['ANTES_20'] = df_fact_av_llamadas_bd.ANTES_20.astype(
                    np.int64)

            elif x == '%_NIVEL_PRODUCTIVIDAD':

                df_fact_av_llamadas['PROM_AGENTES_PRESENTES'] = df_fact_av_llamadas.PROM_AGENTES_PRESENTES.astype(
                    np.float64)
                df_fact_av_llamadas['%_NIVEL_PRODUCTIVIDAD'] = df_fact_av_llamadas['%_NIVEL_PRODUCTIVIDAD'].astype(
                    np.float64)
            else:
                df_fact_av_llamadas_bd[x] = df_fact_av_llamadas_bd[x].astype(
                    df_fact_av_llamadas[x].dtypes.name)

        # Logica para comparar dos dataframes y encontrar las diferencias que se encuentran solo en el de la izquierda (al stagin area)
        df_merge_left = df_fact_av_llamadas.merge(
            df_fact_av_llamadas_bd, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']

        df_merge_left = df_merge_left.drop(['_merge'], axis=1)

        df_merge_left.reset_index(drop=True, inplace=True)

        # Lista de los atributos a eliminar de las diferencias encontradas.
        fecha_list = df_merge_left['FECHA'].tolist()
        id_dim_skill = df_merge_left['ID_DIM_SKILL'].tolist()
        id_dim_intervalo = df_merge_left['ID_DIM_INTERVALO'].tolist()

        for i in range(len(fecha_list)):
            conn.delete_avaya(table='fact_av_llamadas',
                              fecha=fecha_list[i], id_dim_skill=id_dim_skill[i], id_dim_intervalo=id_dim_intervalo[i])

        # Creo la columna "INSERTAR_DT" con la fecha de hoy con la que se insertará al cubo
        today = pd.Timestamp("today").strftime("%Y-%m-%d")

        df_fact_av_llamadas = df_merge_left

        df_fact_av_llamadas['INSERTAR_DT'] = today

        # Parseo fecha que esta en formato obj a datetime.
        df_fact_av_llamadas['INSERTAR_DT'] = pd.to_datetime(
            df_fact_av_llamadas['INSERTAR_DT'])

        df_fact_av_llamadas.to_sql('fact_av_llamadas', con=conn.conecction_db(),
                                   if_exists='append', index=False)

    else:
        pass


def fact_av_abandonos():
    dims_avaya = DimensionesAvaya()
    df_av_abandonos = pd.read_sql(conn.select_table_query(
        '*', 'av_abandonos'), conn.conecction_db())

    if not df_av_abandonos.empty:

        df_av_abandonos = df_av_abandonos.convert_dtypes()

        # Hago el merge de los dataframes vs las dimensiones por sus campos que comparten.
        df_fact_av_abandonos = pd.merge(df_av_abandonos, dims_avaya.dim_skill, left_on='SPLIT_SKILL', right_on='NUMERO_SKILL').merge(
            dims_avaya.dim_intervalo, left_on='INTERVALO', right_on='INICIO_INTERVALO')

        columns_bd = conn.select_columns_table('fact_av_abandonos')
        columns_bd.pop(0)

        columns_df = (df_fact_av_abandonos.columns).to_list()

        # Elimino las columnas que sobran del DF dejando solo las columnas necesarias para insertar en la BD, esto se hace con list comprenhensions.
        df_fact_av_abandonos = df_fact_av_abandonos.drop(
            [columna for columna in columns_df if columna not in columns_bd], axis=1)

        df_fact_av_abandonos_bd = pd.read_sql(conn.select_table_query(
            column='*', table='fact_av_abandonos'), conn.conecction_db())

        df_fact_av_abandonos_bd = df_fact_av_abandonos_bd.drop(
            ['ID_FACT_AV_ABANDONOS', 'INSERTAR_DT'], axis=1)

        # Logica para comparar dos dataframes y encontrar las diferencias que se encuentran solo en el de la izquierda (al stagin area)
        df_merge_left = df_fact_av_abandonos.merge(
            df_fact_av_abandonos_bd, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']

        df_merge_left = df_merge_left.drop(['_merge'], axis=1)

        df_merge_left.reset_index(drop=True, inplace=True)

        # Lista de los atributos a eliminar de las diferencias encontradas.
        fecha_list = df_merge_left['FECHA'].tolist()
        id_dim_skill = df_merge_left['ID_DIM_SKILL'].tolist()
        id_dim_intervalo = df_merge_left['ID_DIM_INTERVALO'].tolist()

        for i in range(len(fecha_list)):
            conn.delete_avaya(table='fact_av_abandonos',
                              fecha=fecha_list[i], id_dim_skill=id_dim_skill[i], id_dim_intervalo=id_dim_intervalo[i])

        # Creo la columna "INSERTAR_DT" con la fecha de hoy con la que se insertará al cubo
        today = pd.Timestamp("today").strftime("%Y-%m-%d")

        df_fact_av_abandonos = df_merge_left

        df_fact_av_abandonos['INSERTAR_DT'] = today

        # Parseo fecha que esta en formato obj a datetime.
        df_fact_av_abandonos['INSERTAR_DT'] = pd.to_datetime(
            df_fact_av_abandonos['INSERTAR_DT'])

        df_fact_av_abandonos.to_sql('fact_av_abandonos', con=conn.conecction_db(),
                                    if_exists='append', index=False)

    else:
        pass
