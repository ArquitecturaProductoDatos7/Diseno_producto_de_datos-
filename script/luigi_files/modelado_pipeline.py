# config: utf8
import json, os, datetime, boto3, luigi, time, pickle
import luigi.contrib.s3
from luigi.contrib.postgres import CopyToTable, PostgresQuery
#from luigi.contrib import rdbms
#from luigi import task
import pandas as pd
import socket
import getpass
import pandas.io.sql as psql
import funciones_rds
import funciones_s3
import funciones_req
import funciones_mod
from etl_pipeline_ver6 import ObtieneRDSHost, InsertaMetadatosPruebasUnitariasClean
from pruebas_unitarias import TestFeatureEngineeringMarbles, TestFeatureEngineeringPandas


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




class CreaTablaFeatuEnginMetadatos(PostgresQuery):
    "Crea la tabla de los metadatos dentro del esquema PROCESAMIENTO"
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
            CREATE TABLE procesamiento.Metadatos(fecha_de_ejecucion VARCHAR,
                                          ip_address VARCHAR,
                                          usuario VARCHAR,
                                          task_id VARCHAR,
                                          task_status VARCHAR,
                                          columnas_generadas VARCHAR,
                                          columnas_recategorizadas VARCHAR,
                                          columnas_imputadas VARCHAR,
                                          columnas_one_hote_encoder VARCHAR
                                          ); 
            """

    def requires(self):
         return CreaEsquemaProcesamiento(self.db_instance_id, self.subnet_group, self.security_group,
                                   self.host, self.database, self.user, self.password)





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
            CREATE TABLE modelo.Metadatos(fecha_de_ejecucion VARCHAR,
                                          ip_address VARCHAR,
                                          usuario VARCHAR,
                                          archivo_modelo VARCHAR,
                                          archivo_metadatos VARCHAR,
                                          mean_fit_time VARCHAR,
                                          std_fit_time VARCHAR,
                                          mean_score_time VARCHAR,
                                          std_score_time VARCHAR,
                                          params VARCHAR,
                                          split0_test_score VARCHAR,
                                          split1_test_score VARCHAR,
                                          split2_test_score VARCHAR,
                                          split3_test_score VARCHAR,
                                          split4_test_score VARCHAR,
                                          split5_test_score VARCHAR,
                                          split6_test_score VARCHAR,
                                          split7_test_score VARCHAR,
                                          split8_test_score VARCHAR,
                                          split9_test_score VARCHAR,
                                          mean_test_score VARCHAR,
                                          std_test_score VARCHAR,
                                          rank_test_score VARCHAR,
                                          modelo VARCHAR
                                          ); 
            """

    def requires(self):
         return CreaEsquemaModelo(self.db_instance_id, self.subnet_group, self.security_group,
                                   self.host, self.database, self.user, self.password) 




class CreaTablaModeloMetadatosParaMejorModelo(PostgresQuery):
    "Crea la tabla de metadatos_mejor_modelo dentro del esquema Modelo"
    #Para la creacion de la base
    db_instance_id = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()

    #Para conectarse a la base
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    host = luigi.Parameter()

    table = ""
    query = """
            CREATE TABLE modelo.Metadatos_mejor_modelo(fecha_de_ejecucion VARCHAR,
                                                       ip_address VARCHAR,
                                                       usuario VARCHAR,
                                                       archivo_modelo VARCHAR,
                                                       archivo_metadatos VARCHAR,
                                                       precision_score FLOAT,
                                                       parametros VARCHAR
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
        return luigi.LocalTarget('1.MODELADO_CreaBucket.txt')






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


    pre_path = '1.preprocesamiento'
    file_name = 'base_procesada'

    def requires(self):
       return [InsertaMetadatosPruebasUnitariasClean(),
               CreaBucket(self.bucket)]

    def run(self):
       host = funciones_rds.db_endpoint(self.db_instance_id)

       dataframe = funciones_rds.obtiene_df(self.db_name, self.db_user_name, self.db_user_password, host)
       dataframe = funciones_mod.preprocesamiento_variable(dataframe)
       dataframe = funciones_mod.crea_variable_target(dataframe)
       dataframe =  funciones_mod.elimina_na_de_variable_delegacion(dataframe)

       #print("Df que vamos a guardar\n",dataframe.head())

       ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
       s3_resource = ses.resource('s3')
       obj = s3_resource.Bucket(self.bucket)

       with self.output().open('w') as out_file:
            dataframe.to_csv(out_file, sep='\t', encoding='utf-8', index=None)


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

    #Para la tarea actual
    folder_path = '2.separacion_base'

    def requires(self):
        return PreprocesoBase(self.db_instance_id, self.db_name, self.db_user_name,
                              self.db_user_password, self.subnet_group, self.security_group,
                              self.bucket, self.root_path)


    def run(self):

       with self.input().open('r') as infile:
             dataframe = pd.read_csv(infile, sep="\t")
            #print('Pude leer el csv\n' , dataframe.head(5))
             vars_modelo = ['delegacion_inicio','mes','dia_semana','hora', 'tipo_entrada', 'incidente_c4_rec', 'target']
             var_target = 'target'
             [X_train, X_test, y_train, y_test] = funciones_mod.separa_train_y_test(dataframe, vars_modelo, var_target)


       ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
       s3_resource = ses.resource('s3')
       obj = s3_resource.Bucket(self.bucket)

       with self.output()['X_train'].open('w') as outfile1:
            X_train.to_csv(outfile1, sep='\t', encoding='utf-8', index=None)

       with self.output()['X_test'].open('w') as outfile2:
           X_test.to_csv(outfile2, sep='\t', encoding='utf-8', index=None)

       with self.output()['y_train'].open('w') as outfile3:
           y_train.to_csv(outfile3, sep='\t', encoding='utf-8', index=None)

       with self.output()['y_test'].open('w') as outfile4:
           y_test.to_csv(outfile4, sep='\t', encoding='utf-8', index=None)


    def output(self):
       output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path
                            )
       return {'X_train':luigi.contrib.s3.S3Target(path=output_path+'X_train.csv'),
               'X_test':luigi.contrib.s3.S3Target(path=output_path+'X_test.csv'),
               'y_train':luigi.contrib.s3.S3Target(path=output_path+'y_train.csv'),
               'y_test':luigi.contrib.s3.S3Target(path=output_path+'y_test.csv'),
               }






class ImputacionesBase(luigi.Task):
    "Esta tarea hace la imputacion de la base en la Train & Test"

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

    #Para la tarea actual
    folder_path = '3.imputaciones'

    def requires(self):

       return {'infiles': SeparaBase(self.db_instance_id, self.db_name, self.db_user_name,
                                     self.db_user_password, self.subnet_group, self.security_group,
                                     self.bucket, self.root_path)}


    def run(self):
       #Se abren los archivos
       with self.input()['infiles']['X_train'].open('r') as infile1:
             X_train = pd.read_csv(infile1, sep="\t")
       with self.input()['infiles']['X_test'].open('r') as infile2:
             X_test = pd.read_csv(infile2, sep="\t")
       with self.input()['infiles']['y_train'].open('r') as infile3:
             y_train = pd.read_csv(infile3, sep="\t")
       with self.input()['infiles']['y_test'].open('r') as infile4:
             y_test = pd.read_csv(infile4, sep="\t")

       #Se realizan las imputaciones
       [X_train, X_test] = funciones_mod.imputacion_variable_delegacion(X_train, X_test)

       #Se guardan los archivos
       with self.output()['X_train'].open('w') as outfile1:
            X_train.to_csv(outfile1, sep='\t', encoding='utf-8', index=None)
       with self.output()['X_test'].open('w') as outfile2:
           X_test.to_csv(outfile2, sep='\t', encoding='utf-8', index=None)
       with self.output()['y_train'].open('w') as outfile3:
           y_train.to_csv(outfile3, sep='\t', encoding='utf-8', index=None)
       with self.output()['y_test'].open('w') as outfile4:
           y_test.to_csv(outfile4, sep='\t', encoding='utf-8', index=None)



    def output(self):
       output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path,
                            )
       return {'X_train':luigi.contrib.s3.S3Target(path=output_path+'X_train.csv'),
               'X_test':luigi.contrib.s3.S3Target(path=output_path+'X_test.csv'),
               'y_train':luigi.contrib.s3.S3Target(path=output_path+'y_train.csv'),
               'y_test':luigi.contrib.s3.S3Target(path=output_path+'y_test.csv'),
               }






class DummiesBase(luigi.Task):
    "Esta tarea convierte las variables categoricas a dummies (One-hot encoder) para la base Train & Test"

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

    #Para la tarea actual
    folder_path = '4.input_modelo'

    def requires(self):

       return {'infiles': ImputacionesBase(self.db_instance_id, self.db_name, self.db_user_name,
                                           self.db_user_password, self.subnet_group, self.security_group,
                                           self.bucket, self.root_path)}


    def run(self):
       #Se abren los archivos
       with self.input()['infiles']['X_train'].open('r') as infile1:
             X_train = pd.read_csv(infile1, sep="\t")
       with self.input()['infiles']['X_test'].open('r') as infile2:
             X_test = pd.read_csv(infile2, sep="\t")
       with self.input()['infiles']['y_train'].open('r') as infile3:
             y_train = pd.read_csv(infile3, sep="\t")
       with self.input()['infiles']['y_test'].open('r') as infile4:
             y_test = pd.read_csv(infile4, sep="\t")

       #Se hace el one-hot encoder y se obtiene la base lista para el modelo
       [X_train_input, X_test_input] = funciones_mod.dummies_para_categoricas(X_train, X_test)

       #Se guardan los archivos
       with self.output()['X_train'].open('w') as outfile1:
            X_train_input.to_csv(outfile1, sep='\t', encoding='utf-8', index=None)
       with self.output()['X_test'].open('w') as outfile2:
           X_test_input.to_csv(outfile2, sep='\t', encoding='utf-8', index=None)
       with self.output()['y_train'].open('w') as outfile3:
           y_train.to_csv(outfile3, sep='\t', encoding='utf-8', index=None)
       with self.output()['y_test'].open('w') as outfile4:
           y_test.to_csv(outfile4, sep='\t', encoding='utf-8', index=None)



    def output(self):
       output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path,
                            )
       return {'X_train':luigi.contrib.s3.S3Target(path=output_path+'X_train_input.csv'),
               'X_test':luigi.contrib.s3.S3Target(path=output_path+'X_test_input.csv'),
               'y_train':luigi.contrib.s3.S3Target(path=output_path+'y_train.csv'),
               'y_test':luigi.contrib.s3.S3Target(path=output_path+'y_test.csv'),
               }




class InsertaMetadatosFeatuEngin(CopyToTable):
    """
    Esta funcion inserta los metadatos de Feature Engineering
    """
    #Para la creacion de la base
    db_instance_id = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group =  luigi.Parameter()
#    host = 'db-dpa20.clkxxfkka82h.us-east-1.rds.amazonaws.com'
    host =  luigi.Parameter()
    # Parametros del Bucket
    bucket = luigi.Parameter()
    root_path = luigi.Parameter()

    #Data 
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    date_time = datetime.datetime.now()
    task = 'InsertaMetadatosFeatuEngin'

    fecha_de_ejecucion = date_time.strftime("%d/%m/%Y %H:%M:%S")
    ip_address = ip_address
    usuario = getpass.getuser()
    task_id = task
    task_status = 'Success'
    columnas_generadas ='incidente_c4_rec,hora,clave,target'
    columnas_recategorizadas ='incidente_c4'
    columnas_imputadas ='delegacion_inicio'
    columnas_one_hot_encoder ='delegacion_inicio,dia_semana,tipo_entrada'

    table = "procesamiento.Metadatos"

    columns=[("fecha_de_ejecucion", "VARCHAR"),
             ("ip_address", "VARCHAR"),
             ("usuario", "VARCHAR"),
             ("task_id", "VARCHAR"),
             ("task_status", "VARCHAR"),
             ("columnas_generadas", "VARCHAR"),
             ("columnas_recategorizadas", "VARCHAR"),
             ("columnas_imputadas", "VARCHAR"),
             ("columnas_one_hote_encoder", "VARCHAR")]

    def rows(self):
        r=[(self.fecha_de_ejecucion,self.ip_address,self.usuario,self.task_id,self.task_status,
            self.columnas_generadas,self.columnas_recategorizadas,self.columnas_imputadas,self.columnas_one_hot_encoder)]
        return(r)

    def requires(self):
        # Indica que se debe hacer primero las tareas especificadas aqui
        return  [CreaTablaFeatuEnginMetadatos(self.db_instance_id, self.subnet_group, self.security_group, self.host,
                                              self.database, self.user, self.password),
                 DummiesBase(self.db_instance_id, self.database, self.user,
                             self.password, self.subnet_group, self.security_group,
                             self.bucket, self.root_path)]


class Test1ForFeatureEngineering(luigi.Task):
    "Corre las pruebas unitarias para la parte de Feature Engineering"

    db_instance_id = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    host = luigi.Parameter()

    bucket = luigi.Parameter()
    root_path = luigi.Parameter()
    folder_path = luigi.Parameter()

    def requires(self):
        return InsertaMetadatosFeatuEngin(self.db_instance_id, self.db_name, self.db_user_name,
                                          self.db_user_password, self.subnet_group, self.security_group,
                                          self.host, self.bucket, self.root_path)

    def run(self):
        prueba_feature_engineering_marbles = TestFeatureEngineeringMarbles()
        prueba_feature_engineering_marbles.test_uniques_incidente_c4_rec()
        metadatos=funciones_req.metadata_para_pruebas_unitarias('test_uniques_incidente_c4_rec','success','feature_engineering')

        with self.output().open('w') as out_file:
            metadatos.to_csv(out_file, sep='\t', encoding='utf-8', index=None, header=False)

    def output(self):
        output_path = "s3://{}/{}/{}/".\
                    format(self.bucket,
                           self.root_path,
                           self.folder_path
                           )
        return luigi.contrib.s3.S3Target(path=output_path+"metadatos_prueba1_FE.csv")


class Test2ForFeatureEngineering(luigi.Task):
    "Corre las pruebas unitarias para la parte de Feature Engineering"

    db_instance_id = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    host = luigi.Parameter()

    bucket = luigi.Parameter()
    root_path = luigi.Parameter()
    folder_path = luigi.Parameter()

    def requires(self):
        return InsertaMetadatosFeatuEngin(self.db_instance_id, self.db_name, self.db_user_name,
                                          self.db_user_password, self.subnet_group, self.security_group,
                                          self.host, self.bucket, self.root_path)
    def run(self):
        prueba_feature_engineering_marbles = TestFeatureEngineeringMarbles()
        prueba_feature_engineering_marbles.test_nulls_x_train()
        metadatos=funciones_req.metadata_para_pruebas_unitarias('test_nulls_x_train','success','feature_engineering')

        with self.output().open('w') as out_file:
            metadatos.to_csv(out_file, sep='\t', encoding='utf-8', index=None, header=False)

    def output(self):
        output_path = "s3://{}/{}/{}/".\
                    format(self.bucket,
                           self.root_path,
                           self.folder_path
                           )
        return luigi.contrib.s3.S3Target(path=output_path+"metadatos_prueba2_FE.csv")



class Test3ForFeatureEngineering(luigi.Task):
    "Corre las pruebas unitarias para la parte de Feature Engineering"

    db_instance_id = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    host = luigi.Parameter()

    bucket = luigi.Parameter()
    root_path = luigi.Parameter()
    folder_path = luigi.Parameter()

    def requires(self):
        return InsertaMetadatosFeatuEngin(self.db_instance_id, self.db_name, self.db_user_name,
                                          self.db_user_password, self.subnet_group, self.security_group,
                                          self.host, self.bucket, self.root_path)

    def run(self):
        prueba_feature_engineering_pandas = TestFeatureEngineeringPandas()
        prueba_feature_engineering_pandas.test_num_columns_x_train()
        metadatos=funciones_req.metadata_para_pruebas_unitarias('test_num_columns_x_train','success','feature_engineering')

        with self.output().open('w') as out_file:
            metadatos.to_csv(out_file, sep='\t', encoding='utf-8', index=None, header=False)

    def output(self):
        output_path = "s3://{}/{}/{}/".\
                    format(self.bucket,
                           self.root_path,
                           self.folder_path
                           )
        return luigi.contrib.s3.S3Target(path=output_path+"metadatos_prueba3_FE.csv")





class Test4ForFeatureEngineering(luigi.Task):
    "Corre las pruebas unitarias para la parte de Feature Engineering"

    db_instance_id = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    host = luigi.Parameter()

    bucket = luigi.Parameter()
    root_path = luigi.Parameter()
    folder_path = luigi.Parameter()

    def requires(self):
        return InsertaMetadatosFeatuEngin(self.db_instance_id, self.db_name, self.db_user_name,
                                          self.db_user_password, self.subnet_group, self.security_group,
                                          self.host, self.bucket, self.root_path)

    def run(self):
        prueba_feature_engineering_pandas = TestFeatureEngineeringPandas()
        prueba_feature_engineering_pandas.test_numerical_columns_x_train()
        metadatos=funciones_req.metadata_para_pruebas_unitarias('test_numerical_columns_x_train','success','feature_engineering')

        with self.output().open('w') as out_file:
            metadatos.to_csv(out_file, sep='\t', encoding='utf-8', index=None, header=False)

    def output(self):
        output_path = "s3://{}/{}/{}/".\
                    format(self.bucket,
                           self.root_path,
                           self.folder_path
                           )
        return luigi.contrib.s3.S3Target(path=output_path+"metadatos_prueba4_FE.csv")







class InsertaMetadatosPruebasUnitariasFeatureEngin(CopyToTable):
    "Inserta los metadatos para las pruebas unitarias en Feature Engineering" 
    # Parametros del RDS
    db_instance_id = 'db-dpa20'
    subnet_group = 'subnet_gp_dpa20'
    security_group = 'sg-09b7d6fd6a0daf19a'
    # Para condectarse a la Base
    database = 'db_incidentes_cdmx'
    user = 'postgres'
    password = 'passwordDB'
    host = funciones_rds.db_endpoint(db_instance_id)

    bucket = 'dpa20-incidentes-cdmx'
    root_path = 'bucket_incidentes_cdmx'
    folder_path = '0.pruebas_unitarias'


    # Nombre de la tabla a insertar
    table = 'tests.pruebas_unitarias'

    # Estructura de las columnas que integran la tabla (ver esquema)
    columns=[("fecha_ejecucion", "VARCHAR"),
             ("ip_address", "VARCHAR"),
             ("usuario", "VARCHAR"),
             ("test", "VARCHAR"),
             ("test_status", "VARCHAR"),
             ("level", "VARCHAR")]

    def rows(self):
         for file in ["infile1", "infile2", "infile3", "infile4"]:
             #Leemos el df de metadatos
             with self.input()[file].open('r') as infile:
                  for line in infile:
                      yield line.strip("\n").split("\t")


    def requires(self):
        return  { "infile1": Test1ForFeatureEngineering(self.db_instance_id, self.database, self.user, self.password,
                                                       self.subnet_group, self.security_group,self.host,
                                                       self.bucket, self.root_path, self.folder_path),
                  "infile2": Test2ForFeatureEngineering(self.db_instance_id, self.database, self.user, self.password,
                                                       self.subnet_group, self.security_group,self.host, 
                                                       self.bucket, self.root_path, self.folder_path),
                  "infile3": Test3ForFeatureEngineering(self.db_instance_id, self.database, self.user, self.password,
                                                       self.subnet_group, self.security_group,self.host, 
                                                       self.bucket, self.root_path, self.folder_path),
                  "infile4": Test4ForFeatureEngineering(self.db_instance_id, self.database, self.user, self.password,
                                                       self.subnet_group, self.security_group,self.host, 
                                                       self.bucket, self.root_path, self.folder_path)}




class ModeloRandomForest(luigi.Task):
    """
    Esta tarea ejecuta el modelo de Ranfom Forest con los parametros que recibe luigi y genera el pickle que se guarda en S3
          n estimators = entero. The number of trees in the forest.
          max depth = entero. The maximum depth of the tree.
          max features = int, float, string or none. 
          min samples split = int, float, optional
          min samples leaf = int, float, optional
    """

    #Parametros para el modelo
    n_estimators = luigi.IntParameter()
    max_depth = luigi.IntParameter()
    max_features = luigi.Parameter()
    min_samples_split = luigi.IntParameter()
    min_samples_leaf = luigi.IntParameter()

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

    #Folder para guardar la tarea actual en el s3
    folder_path = '5.modelo'
    fname = ""

    def requires(self):

       return {'infiles': DummiesBase(self.db_instance_id, self.db_name, self.db_user_name,
                                      self.db_user_password, self.subnet_group, self.security_group,
                                      self.bucket, self.root_path),
               'infiles2': InsertaMetadatosPruebasUnitariasFeatureEngin()}


    def run(self):
       #Se abren los archivos
       with self.input()['infiles']['X_train'].open('r') as infile1:
             X_train_input = pd.read_csv(infile1, sep="\t")
       with self.input()['infiles']['X_test'].open('r') as infile2:
             X_test_input = pd.read_csv(infile2, sep="\t")
       with self.input()['infiles']['y_train'].open('r') as infile3:
             y_train = pd.read_csv(infile3, sep="\t")
       with self.input()['infiles']['y_test'].open('r') as infile4:
             y_test = pd.read_csv(infile4, sep="\t")

       #Grid de hiper-parametros para el modelo
       hyper_params_grid= {'n_estimators': [self.n_estimators],
                          'max_depth': [self.max_depth],
                          'max_features': [self.max_features],
                          'min_samples_split': [self.min_samples_split],
                          'min_samples_leaf': [self.min_samples_leaf]}

       print('***** Comienza a calcular el modelo *****')
       #Se corre el modelo
       [metadata, grid_search] = funciones_mod.magic_loop_ramdomF(X_train_input, y_train, hyper_params_grid)

       self.fname = "random_forest_n_estimators_" + str(self.n_estimators) + "_max_depth_" + str(self.max_depth) + "_max_features_" + str(self.max_features) + "_min_samples_split_" + str(self.min_samples_split) + "_min_samples_leaf_" + str(self.min_samples_leaf)
       metadata = funciones_mod.completa_metadatos_modelo(metadata, self.fname)

       #Se guardan los archivos
       with self.output().open('w') as outfile1:
           metadata.to_csv(outfile1, sep='\t', encoding='utf-8', index=None, header=False)

       with open(self.fname+'.pkl', 'wb') as outfile2:
           pickle.dump(grid_search, outfile2)

       funciones_s3.upload_file(self.fname+'.pkl', self.bucket,
                                '{}/{}/{}'.format(self.root_path, self.folder_path, self.fname+'.pkl'))

    def output(self):
       output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path,
                            )

       return luigi.contrib.s3.S3Target(path=output_path+'metadata_'+self.fname+'.csv')






class ModeloRegresion(luigi.Task):
    """
    Esta tarea ejecuta el modelo de Regresión Logística con los parametros que recibe luigi y genera el pickle que se guarda en S3
           penalty = {‘l1’, ‘l2’, ‘elasticnet’, ‘none’}
           C = float
    """

    #Parametros para el modelo
    penalty = luigi.Parameter()
    c_param = luigi.FloatParameter()

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

    #Folder para guardar la tarea actual en el s3
    folder_path = '5.modelo'
    fname = ""

    def requires(self):

       return {'infiles': DummiesBase(self.db_instance_id, self.db_name, self.db_user_name,
                                      self.db_user_password, self.subnet_group, self.security_group,
                                      self.bucket, self.root_path)}


    def run(self):
       #Se abren los archivos
       with self.input()['infiles']['X_train'].open('r') as infile1:
             X_train_input = pd.read_csv(infile1, sep="\t")
       with self.input()['infiles']['X_test'].open('r') as infile2:
             X_test_input = pd.read_csv(infile2, sep="\t")
       with self.input()['infiles']['y_train'].open('r') as infile3:
             y_train = pd.read_csv(infile3, sep="\t")
       with self.input()['infiles']['y_test'].open('r') as infile4:
             y_test = pd.read_csv(infile4, sep="\t")

       #Grid de hiper-parametros para el modelo
       hyper_params_grid= {'penalty': [self.penalty],
                          'C': [self.c_param]}

       print('***** Comienza a calcular el modelo *****')
       #Se corre el modelo
       [metadata, grid_search] = funciones_mod.magic_loop_RL(X_train_input, y_train, hyper_params_grid)

       self.fname = "reg_log_penalty_" + str(self.penalty) + "_C_" + str(self.c_param)
       metadata = funciones_mod.completa_metadatos_modelo(metadata,self.fname)

       #Se guardan los archivos
       with self.output().open('w') as outfile1:
           metadata.to_csv(outfile1, sep='\t', encoding='utf-8', index=None, header=False)

       with open(self.fname+'.pkl', 'wb') as outfile2:
           pickle.dump(grid_search, outfile2)

       funciones_s3.upload_file(self.fname+'.pkl', self.bucket,
                                '{}/{}/{}'.format(self.root_path, self.folder_path, self.fname+'.pkl'))

    def output(self):
       output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path,
                            )

       return luigi.contrib.s3.S3Target(path=output_path+'metadata_'+self.fname+'.csv')




class ModeloXGB(luigi.Task):
    """
    Esta tarea ejecuta el modelo de XGboost con los parametros que recibe luigi y genera el pickle que se guarda en S3
         n_estimators = int
         learning_rate = float
         subsample = float (o,1]
         max_depth = int
    """

    #Parametros para el modelo
    n_estimators=luigi.IntParameter()
    learning_rate=luigi.FloatParameter()
    subsample=luigi.FloatParameter()
    max_depth=luigi.IntParameter()

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

    #Folder para guardar la tarea actual en el s3
    folder_path = '5.modelo'
    fname = ''

    def requires(self):

       return {'infiles': DummiesBase(self.db_instance_id, self.db_name, self.db_user_name,
                                      self.db_user_password, self.subnet_group, self.security_group,
                                      self.bucket, self.root_path)}


    def run(self):
       #Se abren los archivos
       with self.input()['infiles']['X_train'].open('r') as infile1:
             X_train_input = pd.read_csv(infile1, sep="\t")
       with self.input()['infiles']['X_test'].open('r') as infile2:
             X_test_input = pd.read_csv(infile2, sep="\t")
       with self.input()['infiles']['y_train'].open('r') as infile3:
             y_train = pd.read_csv(infile3, sep="\t")
       with self.input()['infiles']['y_test'].open('r') as infile4:
             y_test = pd.read_csv(infile4, sep="\t")

       #Grid de hiper-parametros para el modelo
       hyper_params_grid= {'n_estimators': [self.n_estimators],'learning_rate':[self.learning_rate],
                          'subsample':[self.subsample], 'max_depth':[self.max_depth]}


       self.fname = "xgb_n_estimators_" + str(self.n_estimators) + "_learning_rate_" + str(self.learning_rate) + "_subsample_" + str(self.subsample) + "_max_depth_" + str(self.max_depth)

       print('***** Comienza a calcular el modelo *****')
       #Se corre el modelo
       [metadata, grid_search] = funciones_mod.magic_loop_XGB(X_train_input, y_train, hyper_params_grid)
       print(metadata.columns)
       metadata = funciones_mod.completa_metadatos_modelo(metadata, self.fname)
       print(metadata.columns)

       #Se guardan los archivos
       with self.output().open('w') as outfile1:
           metadata.to_csv(outfile1, sep='\t', encoding='utf-8', index=None, header=False)

       with open(self.fname+'.pkl', 'wb') as outfile2:
           pickle.dump(grid_search, outfile2)

       funciones_s3.upload_file(self.fname+'.pkl', self.bucket,
                                '{}/{}/{}'.format(self.root_path, self.folder_path, self.fname+'.pkl'))


    def output(self):
       output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path,
                            )

       return luigi.contrib.s3.S3Target(path = output_path + 'metadata_' + self.fname + '.csv')





class InsertaMetadatosRandomForest(CopyToTable):
    "Esta tarea guarda los metadatos del modelo a la RDS"
    #Parametros del modelo
    n_estimators = luigi.IntParameter()
    max_depth = luigi.IntParameter()
    max_features = luigi.Parameter()
    min_samples_split = luigi.IntParameter()
    min_samples_leaf = luigi.IntParameter()

    # Parametros del RDS
    db_instance_id = 'db-dpa20'
    subnet_group = 'subnet_gp_dpa20'
    security_group = 'sg-09b7d6fd6a0daf19a'
    # Para condectarse a la Base
    database = 'db_incidentes_cdmx'
    user = 'postgres'
    password = 'passwordDB'
    host = funciones_rds.db_endpoint(db_instance_id)

    # Nombre de la tabla a insertar
    table = 'modelo.Metadatos'

    # Estructura de las columnas que integran la tabla (ver esquema)
    columns=[("fecha_de_ejecucion", "VARCHAR"),
             ("ip_address", "VARCHAR"),
             ("usuario", "VARCHAR"),
             ("archivo_modelo", "VARCHAR"),
             ("archivo_metadatos", "VARCHAR"),
             ("mean_fit_time", "VARCHAR"),
             ("std_fit_time", "VARCHAR"),
             ("mean_score_time", "VARCHAR"),
             ("std_score_time", "VARCHAR"),
             ("params", "VARCHAR"),
             ("split0_test_score", "VARCHAR"),
             ("split1_test_score", "VARCHAR"),
             ("split2_test_score", "VARCHAR"),
             ("split3_test_score", "VARCHAR"),
             ("split4_test_score", "VARCHAR"),
             ("split5_test_score", "VARCHAR"),
             ("split6_test_score", "VARCHAR"),
             ("split7_test_score", "VARCHAR"),
             ("split8_test_score", "VARCHAR"),
             ("split9_test_score", "VARCHAR"),
             ("mean_test_score", "VARCHAR"),
             ("std_test_score", "VARCHAR"),
             ("rank_test_score", "VARCHAR"),
             ("modelo", "VARCHAR")]


    def rows(self):
         #Leemos el df de metadatos
         with self.input()['infile2'].open('r') as infile:
              for line in infile:
                  yield line.strip("\n").split("\t")


    def requires(self):
        return  {'infile1' : CreaTablaModeloMetadatos(self.db_instance_id, self.subnet_group, self.security_group, self.host,
                                                         self.database, self.user, self.password),
                 'infile2' : ModeloRandomForest(self.n_estimators, self.max_depth, self.max_features, self.min_samples_split, self.min_samples_leaf)}





class InsertaMetadatosRegresion(CopyToTable):
    """
    Esta tarea guarda los metadatos del modelo de regresion logistica a la RDS
       penalty{‘l1’, ‘l2’, ‘elasticnet’, ‘none’}, default=’l2’
       Cfloat, default=1.0
    """

    #Parametros del modelo
    penalty = luigi.Parameter()
    c_param = luigi.FloatParameter()

    # Parametros del RDS
    db_instance_id = 'db-dpa20'
    subnet_group = 'subnet_gp_dpa20'
    security_group = 'sg-09b7d6fd6a0daf19a'
    # Para condectarse a la Base
    database = 'db_incidentes_cdmx'
    user = 'postgres'
    password = 'passwordDB'
    host = funciones_rds.db_endpoint(db_instance_id)
   # host = 'db-dpa20.clkxxfkka82h.us-east-1.rds.amazonaws.com'

    # Nombre de la tabla a insertar
    table = 'modelo.Metadatos'

    # Estructura de las columnas que integran la tabla (ver esquema)
    columns=[("fecha_de_ejecucion", "VARCHAR"),
             ("ip_address", "VARCHAR"),
             ("usuario", "VARCHAR"),
             ("archivo_modelo", "VARCHAR"),
             ("archivo_metadatos", "VARCHAR"),
             ("mean_fit_time", "VARCHAR"),
             ("std_fit_time", "VARCHAR"),
             ("mean_score_time", "VARCHAR"),
             ("std_score_time", "VARCHAR"),
             ("params", "VARCHAR"),
             ("split0_test_score", "VARCHAR"),
             ("split1_test_score", "VARCHAR"),
             ("split2_test_score", "VARCHAR"),
             ("split3_test_score", "VARCHAR"),
             ("split4_test_score", "VARCHAR"),
             ("split5_test_score", "VARCHAR"),
             ("split6_test_score", "VARCHAR"),
             ("split7_test_score", "VARCHAR"),
             ("split8_test_score", "VARCHAR"),
             ("split9_test_score", "VARCHAR"),
             ("mean_test_score", "VARCHAR"),
             ("std_test_score", "VARCHAR"),
             ("rank_test_score", "VARCHAR"),
             ("modelo", "VARCHAR")]

    def rows(self):
         #Leemos el df de metadatos
         with self.input()['infile2'].open('r') as infile:
              for line in infile:
                  yield line.strip("\n").split("\t")


    def requires(self):
        return  {'infile1' : CreaTablaModeloMetadatos(self.db_instance_id, self.subnet_group, self.security_group, self.host,
                                                      self.database, self.user, self.password),
                 'infile2' : ModeloRegresion(self.penalty, self.c_param)}





class InsertaMetadatosXGB(CopyToTable):
    "Esta tarea guarda los metadatos del modelo de XGboost a la RDS"
    #Parametros del modelo
    n_estimators=luigi.IntParameter()
    learning_rate=luigi.FloatParameter()
    subsample=luigi.FloatParameter()
    max_depth=luigi.IntParameter()


    # Parametros del RDS
    db_instance_id = 'db-dpa20'
    subnet_group = 'subnet_gp_dpa20'
    security_group = 'sg-09b7d6fd6a0daf19a'
    # Para condectarse a la Base
    database = 'db_incidentes_cdmx'
    user = 'postgres'
    password = 'passwordDB'
    host = funciones_rds.db_endpoint(db_instance_id)
   # host = 'db-dpa20.clkxxfkka82h.us-east-1.rds.amazonaws.com'

    # Nombre de la tabla a insertar
    table = 'modelo.Metadatos'

    # Estructura de las columnas que integran la tabla (ver esquema)
    columns=[("fecha_de_ejecucion", "VARCHAR"),
             ("ip_address", "VARCHAR"),
             ("usuario", "VARCHAR"),
             ("archivo_modelo", "VARCHAR"),
             ("archivo_metadatos", "VARCHAR"),
             ("mean_fit_time", "VARCHAR"),
             ("std_fit_time", "VARCHAR"),
             ("mean_score_time", "VARCHAR"),
             ("std_score_time", "VARCHAR"),
             ("params", "VARCHAR"),
             ("split0_test_score", "VARCHAR"),
             ("split1_test_score", "VARCHAR"),
             ("split2_test_score", "VARCHAR"),
             ("split3_test_score", "VARCHAR"),
             ("split4_test_score", "VARCHAR"),
             ("split5_test_score", "VARCHAR"),
             ("split6_test_score", "VARCHAR"),
             ("split7_test_score", "VARCHAR"),
             ("split8_test_score", "VARCHAR"),
             ("split9_test_score", "VARCHAR"),
             ("mean_test_score", "VARCHAR"),
             ("std_test_score", "VARCHAR"),
             ("rank_test_score", "VARCHAR"),
             ("modelo", "VARCHAR")]

    def rows(self):
         #Leemos el df de metadatos
         with self.input()['infile'].open('r') as infile:
              for line in infile:
                  yield line.strip("\n").split("\t")


    def requires(self):
        return  {'infile' : ModeloXGB(self.n_estimators, self.learning_rate, self.subsample, self.max_depth)}






class CorreModelos(luigi.task.WrapperTask):
    """
    Esta tarrea corre 3 modelos (Random Forest, Regresion Logistica, XGBoost) con diferentes parametros
    """
    #Parametros del modelo random forest
    n_estimators_rf = [50]
    max_depth_rf = [50, 100]
    max_features = ["sqrt"]
    min_samples_split = [10, 25]
    min_samples_leaf = [10, 20]
    #Regresion logistica
    penalty = ['l1', 'l2']
    c_param = [0.5, 1, 1.5]
    #XGBoost
    n_estimators = [100]
    learning_rate = [0.25]
    subsample = [1.0]
    max_depth = [10, 15]


    def requires(self):
        #yield [[[[[InsertaMetadatosRandomForest(i,j,k,l,m) for i in self.n_estimators_rf] for j in self.max_depth_rf] for k in self.max_features] for l in self.min_samples_split] for m in self.min_samples_leaf]
        yield [[InsertaMetadatosRegresion(i,j) for i in self.penalty] for j in self.c_param]
        #yield [[[[InsertaMetadatosXGB(i,j,k,l) for i in self.n_estimators] for j in self.learning_rate] for k in self.subsample] for l in self.max_depth]









class SeleccionaMejorModelo(luigi.Task):

    #Parámetros para la rds
    db_instance_id = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    host = luigi.Parameter()

    # Parametros del Bucket
    bucket = 'dpa20-incidentes-cdmx'
    root_path = 'bucket_incidentes_cdmx'
    folder_path = '5.modelo'

    #Folder para guardar la tarea actual en el s3
    folder_path_predicciones = '6.predicciones_prueba'
    folder_modelo_final = '7.modelo_final'
    folder_bias = '8.bias_and_fairness'
    fname = ''

    def requires(self):
        return {'infile1': DummiesBase(self.db_instance_id, self.db_name, self.db_user_name,
                                      self.db_user_password, self.subnet_group, self.security_group,
                                      self.bucket, self.root_path),
                'infile2' : CorreModelos()}


    def run(self):

        with self.input()['infile1']['X_train'].open('r') as infile1:
            X_train = pd.read_csv(infile1, sep="\t")
        with self.input()['infile1']['X_test'].open('r') as infile2:
            X_test = pd.read_csv(infile2, sep="\t")
        with self.input()['infile1']['y_train'].open('r') as infile3:
            y_train = pd.read_csv(infile3, sep="\t")
        with self.input()['infile1']['y_test'].open('r') as infile4:
            y_test = pd.read_csv(infile4, sep="\t")

        #hacemos la consulta para traer el nombre del archivo pickle del mejor modelo
        connection=funciones_rds.connect(self.db_name, self.db_user_name, self.db_user_password, self.host)
        archivo_mejormodelo = psql.read_sql("SELECT archivo_modelo FROM modelo.Metadatos ORDER BY cast(mean_test_score as float) DESC limit 1", connection)
        self.fname = archivo_mejormodelo.to_records()[0][1]
        params = psql.read_sql("SELECT params FROM modelo.Metadatos ORDER BY cast(mean_test_score as float) DESC limit 1", connection)
        params = params.to_records()[0][1]
        #lo extraemos del s3
        ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
        s3_resource = boto3.client('s3')

        path_to_file = '{}/{}/{}'.format(self.root_path, self.folder_path, self.fname)
        response = s3_resource.get_object(Bucket=self.bucket, Key=path_to_file)
        body = response['Body'].read()
        mejor_modelo = pickle.loads(body)

        #hacemos las predicciones de la etiqueta y de las probabilidades
        mejor_modelo.fit(X_train, y_train.values.ravel())
        ynew_proba = mejor_modelo.predict_proba(X_test)
        ynew_etiqueta = mejor_modelo.predict(X_test)
        metadata = funciones_mod.metadata_modelo(self.fname, y_test, ynew_etiqueta, params)

        #df para bias y fairness
        df_aux = funciones_mod.hace_df_para_ys(ynew_proba, ynew_etiqueta, y_test)
        x_test_sin_dummies = funciones_mod.dummies_a_var_categorica(X_test, ['delegacion_inicio', 'dia_semana', 'tipo_entrada', 'incidente_c4_rec'])
        df_bias = pd.concat([x_test_sin_dummies, df_aux], axis=1)

        #df para Predicciones
        df_predicciones = pd.concat([x_test_sin_dummies.assign(ano=2020), df_aux], axis=1)
        df_predicciones.drop(['y_test'], axis=1, inplace=True)


        #guardamos las prediciones para X_test
        with self.output()['outfile1'].open('w') as outfile1:
            df_predicciones.to_csv(outfile1, sep='\t', encoding='utf-8', index=None, header=False)

        #guardamos el pickle del mejor modelo
        with self.output()['outfile2'].open('w') as outfile2:
            pickle.dump(archivo_mejormodelo, outfile2)

        #guardamos el df para bias y fairness
        with self.output()['outfile3'].open('w') as outfile3:
            df_bias.to_csv(outfile3, sep='\t', encoding='utf-8', index=None)

        #guardamos el metadata del modelo
        with self.output()['outfile4'].open('w') as outfile4:
            metadata.to_csv(outfile4, sep='\t', encoding='utf-8', index=None, header=False)


    def output(self):
        output_path = "s3://{}/{}/".\
                             format(self.bucket,
                             self.root_path
                            )
        return {'outfile1' : luigi.contrib.s3.S3Target(path=output_path+self.folder_path_predicciones+'/predicciones_modelo.csv'),
                'outfile2' : luigi.contrib.s3.S3Target(path=output_path+self.folder_modelo_final+'/'+self.fname, format=luigi.format.Nop),
                'outfile3' : luigi.contrib.s3.S3Target(path=output_path+self.folder_bias+'/df_bias.csv'),
                'outfile4' : luigi.contrib.s3.S3Target(path=output_path+self.folder_modelo_final+'/metadata_mejor_modelo.csv')}






class InsertaMetadatosMejorModelo(CopyToTable):
    "Inserta las predicciones para las predicciones del set de prueba - X_test" 
    # Parametros del RDS
    db_instance_id = 'db-dpa20'
    subnet_group = 'subnet_gp_dpa20'
    security_group = 'sg-09b7d6fd6a0daf19a'
    # Para condectarse a la Base
    database = 'db_incidentes_cdmx'
    user = 'postgres'
    password = 'passwordDB'
    host = funciones_rds.db_endpoint(db_instance_id)

    # Nombre de la tabla a insertar
    table = 'modelo.Metadatos_mejor_modelo'

    columns = [("fecha_de_ejecucion", "VARCHAR"),
               ("ip_address", "VARCHAR"),
               ("usuario", "VARCHAR"),
               ("archivo_modelo", "VARCHAR"),
               ("archivo_metadatos", "VARCHAR"),
               ("precision_score", "FLOAT"),
               ("parametros", "VARCHAR")]

    def rows(self):
        #Leemos el df de metadatos
        with self.input()['infile1']['outfile4'].open('r') as infile:
              for line in infile:
                  yield line.strip("\n").split("\t")


    def requires(self):
        return  { "infile1" :SeleccionaMejorModelo(self.db_instance_id, self.database, self.user,
                                                   self.password, self.subnet_group, self.security_group, self.host),
                  "infile2" : CreaTablaModeloMetadatosParaMejorModelo(self.db_instance_id, self.subnet_group, self.security_group,
                                                                      self.database, self.user, self.password, self.host)}






class CreaEsquemaPrediccion(PostgresQuery):
    "Crea el esquema Predicciones dentro de la base"
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
    query = "DROP SCHEMA IF EXISTS prediccion cascade; CREATE SCHEMA prediccion;"

    def requires(self):
        return ObtieneRDSHost(self.db_instance_id, self.database, self.user,
                              self.password, self.subnet_group, self.security_group)







class CreaTablaPredicciones(PostgresQuery):
    "Crea la tabla de las predicciones dentro del esquema PREDICCION"
    #Para la creacion de la base
    db_instance_id = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()

    #Para conectarse a la base
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    host = luigi.Parameter()

    table = ""
    query = """
            CREATE TABLE prediccion.Predicciones(id SERIAL PRIMARY KEY,
                                                 mes INT,
                                                 hora INT,
                                                 delegacion_inicio VARCHAR,
                                                 dia_semana VARCHAR,
                                                 tipo_entrada VARCHAR,
                                                 incidente_c4_rec VARCHAR,
                                                 ano INT,
                                                 y_proba_0 FLOAT,
                                                 y_proba_1 FLOAT,
                                                 y_etiqueta INT
                                                 ); 
            """

    def requires(self):
         return CreaEsquemaPrediccion(self.db_instance_id, self.subnet_group, self.security_group,
                                      self.host, self.database, self.user, self.password)






class InsertaPrediccionesPrueba(CopyToTable):
    "Inserta las predicciones para las predicciones del set de prueba - X_test" 
    # Parametros del RDS
    db_instance_id = 'db-dpa20'
    subnet_group = 'subnet_gp_dpa20'
    security_group = 'sg-09b7d6fd6a0daf19a'
    # Para condectarse a la Base
    database = 'db_incidentes_cdmx'
    user = 'postgres'
    password = 'passwordDB'
    host = funciones_rds.db_endpoint(db_instance_id)


    # Nombre de la tabla a insertar
    table = 'prediccion.Predicciones'

    # Estructura de las columnas que integran la tabla (ver esquema)
    columns=[("mes", "INT"),
             ("hora", "INT"),
             ("delegacion_inicio", "VARCHAR"),
             ("dia_semana", "VARCHAR"),
             ("tipo_entrada", "VARCHAR"),
             ("incidente_c4_rec", "VARCHAR"),
             ("ano", "INT"),
             ("y_proba_0", "FLOAT"),
             ("y_proba_1", "FLOAT"),
             ("y_etiqueta", "INT")]

    def rows(self):
        #Leemos el df de metadatos
        with self.input()['infile1']['outfile1'].open('r') as infile:
              for line in infile:
                  yield line.strip("\n").split("\t")


    def requires(self):
        return  { "infile1" :SeleccionaMejorModelo(self.db_instance_id, self.database, self.user,
                                                   self.password, self.subnet_group, self.security_group, self.host),
                  "infile2" : CreaTablaPredicciones(self.db_instance_id, self.subnet_group, self.security_group,
                                                    self.database, self.user, self.password, self.host)}



