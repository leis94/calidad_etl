import win32com.client as win32
from config.config import Conexion
import shutil
import os
import ntpath
import datetime


def xls_2_xlsx():
    # fname = f'C:\Users\Cristian Silva\Documents\Repositorios\etl\data\{file}'
    #fname = r"C:/Users/Cristian Silva/Documents/Repositorios/etl/data/{}".format(file)
    fname = r"C:/Users/Cristian Silva/Documents/Repositorios/etl/data/Llamadas.xls"
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(fname)

    # FileFormat = 51 is for .xlsx extension
    wb.SaveAs(fname+"x", FileFormat=51)
    wb.Close()  # FileFormat = 56 is for .xls extension
    excel.Application.Quit()


def trim_all_columns(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    def trim_strings(x): return x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)


def conver_coma_to_punto_and_float(df, column):
    for name in column:
        df[f'{name}'] = df[f'{name}'].str.replace(',', '.')
        df[f'{name}'] = df[f'{name}'].astype(float)

    return df


def mover_archivo(file_name):

    file_name_without_extension = file_name.rsplit('.', 1)[0]
    today = datetime.datetime.today().strftime('%Y%m%d')
    new_file_name = f"{file_name_without_extension}_{today}.xlsx"

    file_path = f"{os.path.abspath(os.getcwd())}/planos/procesados/{new_file_name}"

    if os.path.exists(file_path):
        os.remove(file_path)

    shutil.move(f"{os.path.abspath(os.getcwd())}/planos/entradas/{file_name}",
                f"{os.path.abspath(os.getcwd())}/planos/procesados/")
    old_file = os.path.join(
        f"{os.path.abspath(os.getcwd())}/planos/procesados/", file_name)
    new_file = os.path.join(
        f"{os.path.abspath(os.getcwd())}/planos/procesados/", new_file_name)
    os.rename(old_file, new_file)


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)


conn = Conexion()


def try_catch_decorator(func):

    def wrapper():
        try:
            func()
        except FileNotFoundError:
            if func.__name__ == 'av_llamadas':
                conn.truncate_table(func.__name__)
            elif func.__name__ == 'av_abandonos':
                conn.truncate_table(func.__name__)
            elif func.__name__ == 'clic_abiertos':
                conn.truncate_table(func.__name__)
            elif func.__name__ == 'clic_resueltos':
                conn.truncate_table(func.__name__)

            print(f"La función {func.__name__} no encontró el archivo")
    return wrapper
