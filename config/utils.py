import win32com.client as win32


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
