
# config: utf8
import json, os, datetime, boto3, luigi, requests
import luigi.contrib.s3
from luigi.contrib.postgres import CopyToTable
from luigi.mock import MockTarget
#from luigi.contrib import rdbms
#from luigi.postgres import PostgresQuery
import pandas as pd
import getpass
#import socket   #para ip de metadatos
import funciones_rds
import funciones_s3
import funciones_req



class ImprimeInicio(luigi.Task):
    """ Esta tarea de hace un print para indicar el inicio del ETL"""
    
    task_complete = False

    def run(self):
        print("*****Inicia tarea*****")
        self.task_complete = True

    # Como no genera un output, se especifica un complete para que luigi sepa que ya acabo
    def complete(self):
        return self.task_complete



    
    
class CreaInstanciaRDS(luigi.Task):
    """ Crea la insancia en RDS cuando se tiene el Subnet Group"""
    
    db_instance_id = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    
    
    task_complete = False
    def requires(self):
        return ImprimeInicio()
    
    def run(self):
        exito = funciones_rds.create_db_instance(self.db_instance_id, self.db_name, self.db_user_name, 
                                         self.db_user_password, self.subnet_group, self.security_group)
        print("Exito en create RDS", exito)
        if exito == 1:
           self.task_complete = True
        
    def complete(self):
        return self.task_complete
    
    
    
    
    

class CreaEsquemasBD(luigi.Task):
    """ Crea esquema raw de la base """
    
    db_instance_id = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    
    task_complete = False
    def requires(self):
        return CreaInstanciaRDS(self.db_instance_id, self.db_name, self.db_user_name,
                                self.db_user_password, self.subnet_group, self.security_group)
    
    def run(self):
        db_endpoint = funciones_rds.db_endpoint(self.db_instance_id)
        funciones_rds.create_schemas(self.db_name, self.db_user_name, self.db_user_password, db_endpoint)
        self.task_complete = True

    def complete(self):
        return self.task_complete    
    
    
    
    
class CreaTablasBD(luigi.Task):
    """ Crea tablas del esquema raw """
    db_instance_id = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    
    task_complete = False
    def requires(self):
        return CreaEsquemasBD(self.db_instance_id, self.db_name, self.db_user_name, self.db_user_password, self.subnet_group, self.security_group)
    
    def run(self):
        db_endpoint = funciones_rds.db_endpoint(self.db_instance_id)
        funciones_rds.create_raw_tables(self.db_name, self.db_user_name, self.db_user_password, db_endpoint)
        self.task_complete = True

    def complete(self):
        return self.task_complete    





class ExtraeInfoPrimeraVez(luigi.Task):
    """
    Extrae toda la informacion: desde el inicio (1-Ene-2014) hasta 2 meses antes de la fecha actual
    """
    db_instance_id = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    #Ruta de la API
    data_url = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5"
    #Parametros de fechas
    year = 0
    month = 0
    
    #Para la creacion de la base
#    db_instance_id = 'db-dpa20'
#    db_name = 'db_accidentes_cdmx'
#    db_user_name = 'postgres'
#    db_user_password = 'passwordDB'
#    subnet_group = 'subnet_gp_dpa20'
#    security_group = 'sg-09b7d6fd6a0daf19a'
            
    def requires(self):
        print("este es in require task de info primera vez")
        # Indica que se debe hacer primero las tareas especificadas aqui
        return CreaTablasBD(self.db_instance_id, self.db_name, self.db_user_name, self.db_user_password, self.subnet_group, self.security_group)


    def run(self):
        #Parametros de los datos
        #DATE_START = datetime.date(2014,1,1)
        DATE_START = datetime.date(2020,1,1)
        date_today = datetime.date.today()
        date_end = datetime.date(date_today.year, date_today.month - 2, 1)
        print("esto es una prueba", date_end)

        #periodo de fechas mensuales
        dates = pd.period_range(start=str(DATE_START), end=str(date_end), freq='M')

        for date in dates:
            self.year = date.year
            self.month = date.month
            

            #hacemos el requerimiento para un chunk del los registros
            [records, metadata] = funciones_req.peticion_api_info_mensual(self.data_url, self.month, self.year)
            
            records_rows = [ funciones_req.crea_rows_para_registros(record) for record in records]
            
            metadata_rows = funciones_req.crea_rows_para_metadata(metadata)
                        
            #Este es el archivo de los registros
            with self.output().open('w') as output_records:
                for record in records_rows:
                    output_records.write(record)
                                   
            # Archivo de los metadatos
            #with self.output().open('w') as output_metadata:
            #    output_metadata.write(metadata_rows)
                
 
    def output(self):
       # return MockTarget("BaseTask")
        return luigi.LocalTarget('words.txt')




class InsertaRegistrosAInstanciaRDS(CopyToTable):
    """ Inserta la informacion a RDS """
    
    #Para la creacion de la base
    #db_instance_id = luigi.Parameter()
    #db_name = luigi.Parameter()
    #db_user_name = luigi.Parameter()
    #db_user_password = luigi.Parameter()
    #subnet_group = luigi.Parameter()
    #security_group = luigi.Parameter()
    db_instance_id = 'db-dpa20-2'
    db_name = 'db_accidentes_cdmx'
    db_user_name = 'postgres'
    db_user_password = 'passwordDB'
    subnet_group = 'subnet_gp_dpa20'
    security_group = 'sg-09b7d6fd6a0daf19a'
    
    host = funciones_rds.db_endpoint(db_instance_id)
    database = db_name 
    user = db_user_name
    password =  db_user_password
    schema = 'raw'
    table = 'IncidentesViales'
    
    print("estoy en copyto table")

    columns = [("latitud", "TEXT"),
               ("folio", "TEXT"),
               ("geopoint", "TEXT"),
               ("hora_creacion", "TEXT"),
               ("delegacion_inicio", "TEXT"),
               ("dia_semana", "TEXT"),
               ("fecha_creacion", "TEXT"),
               ("ano", "TEXT"),
               ("tipo_entrada", "TEXT"),
               ("codigo_cierre", "TEXT"),
               ("hora_cierre", "TEXT"),
               ("incidente_c4", "TEXT"),
               ("mes", "TEXT"),
               ("delegacion_cierre", "TEXT"),
               ("fecha_cierre", "TEXT"),
               ("mesdecierre", "TEXT"),
               ("longitud", "TEXT"),
               ("clas_con_f_alarma", "TEXT"),
              ]

    def requires(self):
        return ExtraeInfoPrimeraVez(self.db_instance_id, self.db_name, self.db_user_name, self.db_user_password, self.subnet_group, self.security_group)
        
    def rows(self):
        with self.input().open('r') as fobj:
           for line in fobj:
#               print(line)
               yield line
