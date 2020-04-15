#Este módulo contiene las funciones que se utilizan para hacer transformaciones necesarias: muestra el tipo de cada variable, separa una variable en 2 columnas, elimina variables, cambia el tipo de variable, convierte a minúsculas y quita los acentos.

import numpy as np

def tipo_variables(archivo):
    print('\nTipo de variables:\n', archivo.dtypes) #el tipo de cada variable
    

def separar_variable(archivo,variable_separar,variable1_nueva,variable2_nueva,separador):
    archivo[variable1_nueva],archivo[variable2_nueva]=archivo[variable_separar].str.split(separador).str


def eliminar_variable(archivo,variable):
    archivo=archivo.drop(variable, axis='columns',inplace=True)
    

def cambiar_tipo_variable(archivo,variable,tipo):
    archivo[variable]=archivo[variable].astype(tipo)


def cambiar_minusculas_variable(archivo,variable):
    archivo[variable]=archivo[variable].str.lower()
    
def quitar_acentos(archivo):
    cols = archivo.select_dtypes(include=[np.object]).columns
    archivo[cols] = archivo[cols].apply(lambda x: x.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8'))
