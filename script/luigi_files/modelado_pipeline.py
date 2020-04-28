# config: utf8
import json, os, datetime, boto3, luigi, time
import luigi.contrib.s3
from luigi.contrib.postgres import CopyToTable, PostgresQuery
#from luigi.contrib import rdbms
#from luigi import task
import pandas as pd
#from numpy import ndarray as np
#import getpass
import funciones_rds
import funciones_s3
import funciones_req
import funciones_mod
from etl_pipeline_ver6 import ETLpipeline, ObtieneRDSHost


class CreaEsquemaProcesamiento(PostgresQuery):
    "Crea el esquema Procesamiento dentro de la base"
    #Para la creacion de la base
    db_instance_id = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()

    #Para conectarse a la base
    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()

    table = ""
    query = "DROP SCHEMA IF EXISTS procesamiento cascade; CREATE SCHEMA procesamiento;"

    def requires(self):
        return ObtieneRDSHost(self.db_instance_id, self.database, self.user,
                              self.password, self.subnet_group, self.security_group)

    

class CreaEsquemaModelo(PostgresQuery):
    "Crea el esquema Modelo dentro de la base"
    #Para la creacion de la base
    db_instance_id = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()

    #Para conectarse a la base
    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()

    table = ""
    query = "DROP SCHEMA IF EXISTS modelo cascade; CREATE SCHEMA modelo;"

    def requires(self):
        return ObtieneRDSHost(self.db_instance_id, self.database, self.user,
                              self.password, self.subnet_group, self.security_group)




class CreaTablaModeloMetadatos(PostgresQuery):
    "Crea la tabla de los metadatos dentro del esquema MODELO"
    #Para la creacion de la base
    db_instance_id = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()

    #Para conectarse a la base
    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()

    table = ""
    query = """
            CREATE TABLE modelo.Metadatos(mean_fit_time FLOAT,
                                          std_fit_time FLOAT,
                                          mean_score_time FLOAT,
                                          std_score_time FLOAT,
                                          param_max_depth INT,
                                          param_max_features VARCHAR,
                                          param_min_samples_leaf INT,
                                          param_min_samples_split INT,
                                          param_n_estimators INT,
                                          params VARCHAR,
                                          split0_test_score FLOAT,
                                          split1_test_score FLOAT,
                                          split2_test_score FLOAT,
                                          split3_test_score FLOAT,
                                          split4_test_score FLOAT,
                                          mean_test_score FLOAT,
                                          std_test_score FLOAT,
                                          rank_test_score INT
                                          ); 
            """

    def requires(self):
         return CreaEsquemaModelo(self.db_instance_id, self.subnet_group, self.security_group,
                                   self.host, self.database, self.user, self.password)





class CreaBucket(luigi.Task):
    """ Esta tarea crea el S3 en AWS """
    priority = 100
    # Nombre del bucket
    bucket = luigi.Parameter()

    def run(self):
        #Crea el bucket
        exit = funciones_s3.create_s3_bucket(self.bucket)
        if exit == 1:
            # Encripta el bucket
            funciones_s3.s3_encriptado(self.bucket)
            # Bloquea acceso publico
            funciones_s3.s3_bloquear_acceso_publico(self.bucket)
            print("***** S3 bucket: properties adjusted *****\n")

        with self.output().open('w') as outfile:
            outfile.write(str(self.bucket))

    def output(self):
        return luigi.LocalTarget('1.MODEL_CreaBucket.txt')





class ObtieneRutaBucket(luigi.Task):
    "Obtiene la ruta del bucket para vrificar su creacion"

    bucket = luigi.Parameter()

    def requires(self):
        return CreaBucket(self.bucket)

    def run(self):
        #Lee el endpoint del archivo
        with self.input().open() as infile, self.output().open('w') as outfile:
             bucket_name = infile.read()
             if self.bucket == bucket_name:
                  print("***** Bucket {} ready *****".format(self.bucket))
                  outfile.write(str(self.bucket))

    def output(self):
        return luigi.LocalTarget("2.MODEL_BucketListo.txt")






class PreprocesoBase(luigi.Task):
    # Parametros del RDS
    db_instance_id = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    # Parametros del Bucket
    bucket = luigi.Parameter()
    root_path = luigi.Parameter()
    pre_path = luigi.Parameter()
    file_name = luigi.Parameter()

    def requires(self):
       return [ETLpipeline(self.db_instance_id , self.db_name, self.db_user_name,
                          self.db_user_password, self.subnet_group, self.security_group),
               ObtieneRutaBucket(self.bucket)]

    def run(self):
       host = funciones_rds.db_endpoint(self.db_instance_id)

       dataframe = funciones_rds.obtiene_df(self.db_name, self.db_user_name, self.db_user_password, host)
       dataframe = funciones_mod.preprocesamiento_variable(dataframe)

       #print("Df que vamos a guardar\n",dataframe.head())

       ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
       s3_resource = ses.resource('s3')
       obj = s3_resource.Bucket(self.bucket)

       with self.output().open('w') as out_file:
            dataframe.to_csv(out_file, sep='\t', encoding='utf-8', index=None)
            out_file.close()

    def output(self):
       output_path = "s3://{}/{}/{}/{}.csv".\
                      format(self.bucket,
                             self.root_path,
                             self.pre_path,
                             self.file_name
                           )
       return luigi.contrib.s3.S3Target(path=output_path)






class SeparaBase(luigi.Task):
    "Esta tarea separa la base en la Train & Test"

    # Parametros del RDS
    db_instance_id = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    # Parametros del Bucket
    bucket = luigi.Parameter()
    root_path = luigi.Parameter()

    #Para la tarea de Preproceso
    pre_path = 'preprocesamiento'
    file_name = 'base_preparada'
    #Para la tarea actual
    folder_path = 'base_separada'
    outfile = ''

    def requires(self):
        return PreprocesoBase(self.db_instance_id, self.db_name, self.db_user_name,
                             self.db_user_password, self.subnet_group, self.security_group,
                             self.bucket, self.root_path, self.pre_path, self.file_name)


    def run(self):

       with self.input().open('r') as infile:
             dataframe = pd.read_csv(infile, sep="\t")
            #print('Pude leer el csv\n' , dataframe.head(5))
             vars_modelo = ['hora_creacion','delegacion_inicio','dia_semana','tipo_entrada','mes','codigo_cierre', 'incidente_c4', 'target']
             var_target = 'target'
             lista = funciones_mod.separa_train_y_test(dataframe, vars_modelo, var_target)


       ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
       s3_resource = ses.resource('s3')
       obj = s3_resource.Bucket(self.bucket)

       lista_names = ['X_train', 'X_test', 'y_train', 'y_test']
       for i in range(0,4):
             self.outfile = str(lista_names[i])
             with self.output().open('w') as self.outfile:
                 df = pd.DataFrame(lista[i])
                 #print(df.head(5))
                 df.to_csv(self.outfile, sep='\t', encoding='utf-8', index=None)
                 self.outfile.close()

    def output(self):
       output_path = "s3://{}/{}/{}/{}.csv".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path,
                             self.outfile
                            )
       return luigi.contrib.s3.S3Target(path=output_path)





class ImputacionesBase(luigi.Task):
    "Esta tarea hace la imputacion de la base en la Train & Test"

    # Parametros del RDS
    db_instance_id = 'db-dpa20'
    db_name = 'db_incidentes_cdmx'
    db_user_name = 'postgres'
    db_user_password = 'passwordDB'
    subnet_group = 'subnet_gp_dpa20'
    security_group = 'sg-09b7d6fd6a0daf19a'
    # Parametros del Bucket
    bucket = 'dpa20-incidentes-cdmx'  #luigi.Parameter()
    root_path = 'bucket_incidentes_cdmx'

    # Parametros del RDS
#    db_instance_id = luigi.Parameter()
#    db_name = luigi.Parameter()
#    db_user_name = luigi.Parameter()
#    db_user_password = luigi.Parameter()
#    subnet_group = luigi.Parameter()
#    security_group = luigi.Parameter()
    # Parametros del Bucket
#    bucket = luigi.Parameter()
#    root_path = luigi.Parameter()

    #Para la tarea actual
    folder_path = 'base_separada'
    outfile = 'EXITO'

#   def requires(self):
#       return SeparaBase(self.db_instance_id, self.db_name, self.db_user_name,
#                         self.db_user_password, self.subnet_group, self.security_group,
#                         self.bucket, self.root_path)


    def run(self):

      ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
#      s3_resource = ses.resource('s3')
      client = boto3.client('s3', region_name='us-east-1')
 
      # These define the bucket and object to read
      file_to_read = 'bucket_incidentes_cdmx/base_separada/X_test.csv' 

      #create a file object using the bucket and object key. 
      fileobj = client.get_object(Bucket=self.bucket, Key=file_to_read) 
      # open the file object and read it into the variable filedata. 
      #filedata = fileobj['Body'].read()
      dataframe = pd.read_csv(fileobj['Body'], sep="\t")
      print(dataframe.head(5))
      print(type(dataframe))
      print(dataframe.columns)
      # file data will be a binary stream. We have to decode it 
      #contents = filedata.decode('utf-8') 

      # Once decoded, you can treat the file as plain text if appropriate 
      #print(contents) 
       


      with self.output().open('w') as self.outfile:
            self.outfile.write("hola")



    def output(self):
      output_path = "s3://{}/{}/{}/{}.csv".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path,
                             self.outfile
                            )
      return luigi.contrib.s3.S3Target(path=output_path)








