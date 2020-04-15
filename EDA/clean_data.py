#Este módulo estandariza el nombre de variables 

def estandariza_variables(archivo):
    columnas_antes=archivo.columns
    archivo.columns=archivo.columns.str.replace(' ','_')
    archivo.columns=archivo.columns.str.replace('ñ','n')
    archivo.columns=archivo.columns.str.replace('Ñ','n')
    archivo.columns=archivo.columns.str.lower()
    print('Antes:\n\n',columnas_antes,'\n','\nAhora:\n\n',archivo.columns)
    