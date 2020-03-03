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
    #CHUNK_SIZE = 10000     # el maximo que permite la API (o tamano del chunk)
    #nrows = CHUNK_SIZE        
    
    #numero total de registros a jalar
    parameters = {'rows': -1, 'refine.mes':mes, 'refine.ano':ano}
    #nrecord = requests.get(url, params = parameters).json()['nhits']
    results = requests.get(url, params = parameters)
    print("******* Estatus ****** ", results.status_code)
    results = results.json()['records']
    results = json.dumps(results)
    print(type(results))
    
    return results
    
        
    
                    
    


def rango_de_fechas(f_ini, f_final):
    """
    Regresa el rango mensual de fechas como una lista
        f_ini, f_final son fechas
    """
    fechas = pd.period_range(start=str(f_ini), end=str(f_final), freq='M')
    
    return fechas


###################################### Versiones anteriores ##################


def extrae_nhits(url):
    res = requests.get(url).json()
    nhits = res['nhits']
    return nhits



def peticion_api_por_chunks(url, start, num_records_total, chunk_size ):
    """
    Esta funcion obtiene los registros de la API por tamano de chunks
        PARAM es un dictionario que especifica el numero de registros y paginas a solicitar. Por ejemplo,
              si queremos solicitar 500 registros de la primera pagina, PARAM = {'rows':500,'start':1}
        num_records_total es el numero de records a solicitar
        chunk size es el tamano del chunk
    """
    
    TOTAL = num_records_total
    NRECORD = start       # lleva la cuenta del registro en que me quede
    NROWS = min(chunk_size, TOTAL)
    
    
    #donde vamos a guardar la informacion obtenida
    results = pd.DataFrame()
           
    PARAMETERS = {'rows':NROWS, 'start':NRECORD}
    raw = requests.get(url, params = PARAMETERS).json()
    results = pd.json_normalize(raw['records'])
    results = results.to_string()
        
    return results



