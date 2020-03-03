import requests
import json
import pandas as pd

DATAURL = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5"


def extrae_nhits(url):
    res = requests.get(url).json()
    nhits = res['nhits']
    return nhits



def peticion_api_por_chunks_parquet(url, start, num_records_total, chunk_size ):
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
    df = pd.io.json.json_normalize(raw['records'])
        
    return df.to_parquet('datos.parquet', engine = 'pyarrow')

