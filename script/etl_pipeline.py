import json
import os
import luigi.contrib.s3
from datetime import date
import boto3

import luigi
import numpy as np

import extrae


TARGET_PATH = os.path.join(os.path.dirname(__file__), 'data/{date}'.format(date=date.today()))
DATAURL = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5"


class extrae_chunk_api_a_S3(luigi.Task)
    """
    Fetches a chunk of information to a JSON file.
    """
    # parametros para chunks
    chunk_size = luigi.parameter.IntParameter(default=10)    # tamano del chunk
    start_record = luigi.parameter.IntParameter(default=1)   # registro en el que comenzamos
    num_records_total = luigi.parameter.IntParameter(default=25) # ultimo registro
    
    #Parametros para la ruta en S3
    bucket = luigi.Parameter()
    root_path = luigi.Parameter()
    etl_path = luigi.Parameter()
    year = luigi.Parameter()
        
    def run(self):
        #hacemos el requerimiento para un chunk del los registros
        response = extrae.peticion_api_por_chunks(DATAURL, self.start_record, self.num_records_total, self.chunk_size)
    
        #guardamos la info en un S3
        ses = boto3.session.Session(profile_name='default', region_name='us-east-2')
        s3_resource = ses.resource('s3')
              
        obj=s3_resource.Bucket(self.bucket)
        
        with self.output().open('w') as fout:
            fout.write(response)
         
        
    def output(self):
        output_path = "s3://{}/{}/{}/YEAR={}/file1.csv".\
        format(self.bucket, 
               self.root_path,
               self.etl_path,
               self.year
              )
        
        return luigi.contrib.s3.S3Target(path=output_path)
    


    
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
    
