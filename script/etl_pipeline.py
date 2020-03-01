import json
import os
from datetime import date

import luigi
import numpy as np

import extrae


TARGET_PATH = os.path.join(os.path.dirname(__file__), 'data/{date}'.format(date=date.today()))
DATAURL = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5"


class extrae_chunk_api(luigi.Task):
    """
    Fetches a chunk of information to a JSON file.
    """
    chunk_size = luigi.parameter.IntParameter(default=10)
    start_record = luigi.parameter.IntParameter(default=1)
        
    def run(self):
        #hacemos el requerimiento para un chunk del los registros
        response = extrae.peticion_api_por_chunks(DATAURL, self.chunk_size, self.start_record)
    
        #lo guardamos en un file        
        with self.output().open('w') as fout:
            fout.write(response)
    
    def output(self):
        return get_local_target("incidentes_viales.csv")
    


    
class extrae_info_api(luigi.Task):
    """
    Fetches all records to a JSON file.
    """
           
        
    def run(self):
        record = 1
        nrecord1 = extrae.extrae_nhits(DATAURL) #total de records a extraer
        nrecord = 25
        
        chunk_size = 10 #10000     # el maximo que permite la API (o tamano del chunk)
        
        file = 1
        nfile = int(np.ceil(nrecord/chunk_size)) # numero max de archivos que tendremos
        
        print('antes del while ', nrecord1, nrecord, nfile)
        
        while record < nrecord:
            if file == nfile:
                # estoy en el ultimo archivo
                nrecord = nrecord - record + 1 #solo los registros que faltan
            
            #hacemos el requerimiento para un chunk del los registros
            response = extrae.peticion_api_por_chunks(DATAURL, record, nrecord , chunk_size)
            print(record, file, "-------------jsdksjdksjd----------hol ------")
            #lo guardamos en un file
            fout = open('file' + str(file) + '.csv',"w") 
            #with self.output().open('a') as fout:
            #with open('file' + str(file) + '.csv', "w") as fout:
            fout.write(response)
            
            #actualiza los contadores
            record = chunk_size*file +1
            file = file + 1
            
            
    
    def output(self):
        return get_local_target("incidentes_viales.csv")
    
#https://luigi.readthedocs.io/en/stable/example_top_artists.html   
#class ArtistToplistToDatabase(luigi.contrib.postgres.CopyToTable):
#    date_interval = luigi.DateIntervalParameter()
#    use_hadoop = luigi.BoolParameter()

#    host = "localhost"
#    database = "toplists"
#    user = "luigi"
#    password = "abc123"  # ;)
#    table = "top10"

#    columns = [("date_from", "DATE"),
#               ("date_to", "DATE"),
#               ("artist", "TEXT"),
#               ("streams", "INT")]

#    def requires(self):
#        return Top10Artists(self.date_interval, self.use_hadoop)
 
    
# https://gist.github.com/tomsing1/4c433655b3ac1aedb372ddfb1c7954db
    
def get_local_target(name):
    return luigi.LocalTarget(os.path.join(TARGET_PATH, name))
    