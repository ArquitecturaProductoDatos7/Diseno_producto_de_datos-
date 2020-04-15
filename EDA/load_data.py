#Este módulo carga la información del archivo indicado e imprime el número de observaciones y variables.

import pandas as pd

def carga_archivo(archivo):
    return pd.read_csv(archivo,sep=';')


def observaciones_variables(archivo):
    print('Número de observaciones:',archivo.shape[0],',', 'Número de variables:',archivo.shape[1],'\n') #numero       de observaciones y variables.
    
    
    
