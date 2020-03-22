import json
import os
import luigi.contrib.s3
import datetime
import boto3
import luigi
import requests
import pandas as pd
import getpass


DATAURL = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5"

class ImprimeInicio(luigi.Task):
    task_complete =False

    def run(self):
        print("*****Inicia tarea*****")
        self.task_complete=True

    def complete(self):
        return self.task_complete

#    def output(self):
#        return []

class peticion_api_info_mensual(luigi.Task):
    """
    Esta funcion obtiene los registros de la API por mes y ano
        mes es un entero para el mes que se quiere: 1,2,3,...,12
        ano es un entero desde 2014 hasta la fecha
    """

    url = luigi.Parameter(default="https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5")
    month = luigi.IntParameter()
    year = luigi.IntParameter()

    bucket = "bucket-dpa-2020"
    root_path = "incidentes_viales_CDMX"
    etl_path = "raw"

    def requires(self):
        return ImprimeInicio()

    def run(self):

        date_start = datetime.date(self.year,self.month,1)
        date_today = datetime.date.today()
        date_end = datetime.date(date_today.year, date_today.month - 2, 31)

        dates = pd.period_range(start=str(date_start), end=str(date_end), freq='M')

        for date in dates:
            self.year = date.year
            self.month = date.month

            #rows = -1 indica todos los registros
            parameters = {'rows': -1, 'refine.mes':self.month, 'refine.ano':self.year}
            print(parameters)
            raw = requests.get(self.url, params = parameters)
            print("******* Estatus ****** ", raw.status_code)
            print(self.year)
            print(self.month)

            #metadata
            metadata = {'fecha_ejecucion': str(date_today),
            'parametros_url': self.url,
            'parametros': parameters,
            'usuario': getpass.getuser(),
            'nombre_archivo': 'incidentes_viales_{}{}.json'.format(self.month,self.year),
            'ruta': 's3://{}/{}/{}/YEAR={}/MONTH={}/'.format(self.bucket, self.root_path, self.etl_path, self.year, self.month),
            'tipo_datos': 'json'
            }
            out = raw.json()
            out['metadata'] = metadata

            #guardamos la info en un S3
            ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
            s3_resource = ses.resource('s3')
            obj = s3_resource.Bucket(self.bucket)

            #results = raw.json()
            with self.output().open('w') as output:
                #output.write(self.raw.json())
                json.dump(out,output)

    def output(self):
        output_path = "s3://{}/{}/{}/YEAR={}/MONTH={}/incidentes_viales_{}{}.json".\
        format(self.bucket, 
               self.root_path,
               self.etl_path,
               self.year,
               self.month,
               self.month,
               self.year
              )
        
        return luigi.contrib.s3.S3Target(path=output_path)


        
    #def output(self):
    #    return luigi.LocalTarget('/home/bruno/Proyectos/incidentes_viales_{}{}.csv'.format(self.mes,self.ano))


class extraeInfoPrimeraVez(luigi.Task):
    """
    Extrae toda la informacion: desde el inicio (1-Ene-2014) hasta 2 meses antes de la fecha actual
    """
    
    #Parametros para la ruta en S3
    bucket = "bucket-dpa-2020"
    root_path = "incidentes_viales_CDMX"
    etl_path = "raw"
    year = 0
    month = 0
    fname = ""

    def requires(self):
        return ImprimeInicio()
            
    def run(self):
        #Parametros de los datos
        #DATE_START = datetime.date(2014,1,1)
        DATE_START = datetime.date(2020,1,1)
        date_today = datetime.date.today()
        date_end = datetime.date(date_today.year, date_today.month - 2, 31)
        
        #periodo de fechas mensuales
        dates = pd.period_range(start=str(DATE_START), end=str(date_end), freq='M')
        
        for date in dates:
            self.year = date.year
            self.month = date.month
            
            #nombre del archivo
            self.fname = "incidentes_viales_" + str(self.month) + str(self.year)
            
            #hacemos el requerimiento para un chunk del los registros
            response = extrae.peticion_api_info_mensual(DATAURL, self.month, self.year)
            
            #guardamos la info en un S3
            ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
            s3_resource = ses.resource('s3')
            
            obj=s3_resource.Bucket(self.bucket)
            
            with self.output().open('w') as self.fname:
                json.dump(response,self.fname)
                #self.fname.write(response)
    
    
    def output(self):
        output_path = "s3://{}/{}/{}/YEAR={}/MONTH={}/{}.json".\
        format(self.bucket, 
               self.root_path,
               self.etl_path,
               self.year,
               self.month,
               self.fname
              )
        
        return luigi.contrib.s3.S3Target(path=output_path)
    



    

