import json
import os
import luigi.contrib.s3
import datetime
import boto3

import luigi
import pandas as pd

import extrae


DATAURL = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5"



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
            
    def run(self):
        #Parametros de los datos
        #DATE_START = datetime.date(2014,1,1)
        DATE_START = datetime.date(2019,8,1)
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
    



    


class extraeInfoMensual(luigi.Task):
    """
    Esta tarea estrae la informacion mensual
    """
    
    #Parametros para la ruta en S3
    bucket = "dpa20"
    root_path = "incidentes_viales_CDMX"
    etl_path = "raw"
    year = 0
    month = 0
    fname = "incidentes_viales_" + str(month) + str(year)
    
    def run(self):
        #hacemos el requerimiento para un chunk del los registros
        response = extrae.peticion_api_info_mensual(DATAURL, self.month, self.year)
    
        #guardamos la info en un S3
        ses = boto3.session.Session(profile_name='default', region_name='us-east-2')
        s3_resource = ses.resource('s3')
              
        obj=s3_resource.Bucket(self.bucket)
        
        with self.output().open('w') as fout:
            fout.write(response)
         
        
    def output(self):
        output_path = "s3://{}/{}/{}/YEAR={}/MONTH={}/{}.csv".\
        format(self.bucket, 
               self.root_path,
               self.etl_path,
               self.year,
               self.month,
               self.fname
              )
        
        return luigi.contrib.s3.S3Target(path=output_path)
