import socket   #para ip de metadatos
import getpass  #para el usuario
import datetime
import requests
import json
import pandas as pd
import numpy as np


def peticion_api_info_mensual(url, mes, ano):
    """
    Esta funcion obtiene los registros de la API por mes y ano, desde el 2014
        mes es un entero para el mes que se quiere: 1,2,3,...,12
        ano es un entero desde 2014 hasta la fecha
    Todos los outputs estan en formato JSON
        records  archivo JSON con los registros
        metadata archivo JSON con la metadata
    """
    hostname = socket.gethostname()     
    ip_address = socket.gethostbyname(hostname)
    date_time = datetime.datetime.now()

    # rows = -1 indica todos los registros 
    parameters = {'rows': -1, 'refine.mes':mes, 'refine.ano':ano}
    # Hacemos el requerimiento de la informacion
    raw = requests.get(url, params = parameters)
    print("******* Estatus ******\n", raw.status_code)
    print("Ano: ", ano, "Mes: ", mes)

    # Se especifica que es tipo json y se separan los records de los parametros
    records = raw.json()['records']
    met_aux = raw.json()['parameters']
    
    metadata = {'dataset': met_aux['dataset'],
                'timezone': met_aux['timezone'], 
                'rows' : met_aux['rows'], 
                'parametro_ano': met_aux['refine']['ano'],
                'parametro_mes': met_aux['refine']['mes'],
                'parametro_url': url,
                'fecha_de_ejecucion': date_time.strftime("%d/%m/%Y %H:%M:%S"), 
                'ip_address': ip_address, 
                'usuario':  getpass.getuser(), 
                'nombre_archivo': 'incidentes_viales_{}{}.json'.format(mes, ano),
                'formato_archivo': met_aux['format'], 
                }
    
    return  records, metadata
    


    

def rango_de_fechas(f_ini, f_final):
    """
    Regresa el rango mensual de fechas como una lista
        f_ini, f_final son fechas
    """
    fechas = pd.period_range(start=str(f_ini), end=str(f_final), freq='M')
    
    return fechas




def crea_rows_para_registros (record):
    """
    Regresa la informacion de los registros en el formato requerido para subirlo a RDA
    """
    l = [json.dumps(record['fields'][campo]) for campo in record['fields'].keys()]
    return l





def crea_rows_para_metadata (meta):
    """
    Regresa la informacion del metadata en el formato requerido para subirlo a RDA
    """
    l = [json.dumps(meta[campo]) for campo in meta.keys()]
    
    return l
