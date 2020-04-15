import requests
import json
import pandas as pd
import numpy as np


def peticion_api_info_mensual(url, mes, ano):
    """
    Esta funcion obtiene los registros de la API por mes y ano
        mes es un entero para el mes que se quiere: 1,2,3,...,12
        ano es un entero desde 2014 hasta la fecha
    """
    #rows = -1 indica todos los registros
    parameters = {'rows': -1, 'refine.mes':mes, 'refine.ano':ano}
    
    raw = requests.get(url, params = parameters)
    print("******* Estatus ****** ", raw.status_code)
    results = raw.json()
    #results = pd.io.json.json_normalize(results['records'])
    #results = results.to_string()
    #results = results.json()['records']
    #results = json.dumps(results)
        
    return results
    
        

def rango_de_fechas(f_ini, f_final):
    """
    Regresa el rango mensual de fechas como una lista
        f_ini, f_final son fechas
    """
    fechas = pd.period_range(start=str(f_ini), end=str(f_final), freq='M')
    
    return fechas
