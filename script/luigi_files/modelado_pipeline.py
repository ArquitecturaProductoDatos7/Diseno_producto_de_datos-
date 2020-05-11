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
from etl_pipeline_ver6 import ETLpipeline, ObtieneRDSHost
import pruebas_unitarias



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
       return [ETLpipeline(self.db_instance_id , self.db_name, self.db_user_name,
                          self.db_user_password, self.subnet_group, self.security_group),
               CreaBucket(self.bucket)]

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
    folder_path = '3.Imputaciones'

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


class TestForFeatureEngineering(luigi.Task):
    
    "Corre las pruebas unitarias para la parte de Feature Engineering"
    
    db_instance_id = 'db-dpa20'
    db_name = 'db_incidentes_cdmx'
    db_user_name = 'postgres'
    db_user_password = 'passwordDB'
    subnet_group = 'subnet_gp_dpa20'
    security_group = 'sg-09b7d6fd6a0daf19a'
    host = funciones_rds.db_endpoint(db_instance_id)
    
    bucket = 'dpa20-incidentes-cdmx'  #luigi.Parameter()
    root_path = 'bucket_incidentes_cdmx'
    folder_path = '0.pruebas_unitarias'
    
    def requires(self):
        return DummiesBase(self.db_instance_id, self.db_name, self.db_user_name,
                           self.db_user_password, self.subnet_group, self.security_group,
                           self.bucket, self.root_path)
    
    def run(self):
        
        prueba_feature_engineering_marbles = pruebas_unitarias.TestFeatureEngineeringMarbles()
        prueba_feature_engineering_marbles.test_uniques_incidente_c4_rec()
        metadatos=funciones_req.metadata_para_pruebas_unitarias('test_uniques_incidente_c4_rec','success','feature_engineering')
        prueba_feature_engineering_marbles.test_nulls_x_train()
        metadatos=metadatos.append(funciones_req.metadata_para_pruebas_unitarias('test_nulls_x_train','success','feature_engineering')
            
        prueba_feature_engineering_pandas = pruebas_unitarias.TestFeatureEngineeringPandas()
        prueba_feature_engineering_pandas.test_num_columns_x_train()
        metadatos=metadatos.append(funciones_req.metadata_para_pruebas_unitarias('test_num_columns_x_train','success','feature_engineering'))
        prueba_feature_engineering_pandas.test_numerical_columns_x_train()
        metadatos=metadatos.append(funciones_req.metadata_para_pruebas_unitarias('test_numerical_columns_x_train','success','feature_engineering'))
        #metadatos=metadatos.reset_index(drop=True)                
                                       
        #ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
        #s3_resource = ses.resource('s3')
        #obj = s3_resource.Bucket(self.bucket)
        with self.output().open('w') as out_file:
            metadatos.to_csv(out_file, sep='\t', encoding='utf-8', index=None)
    
    def output(self):
        output_path = "s3://{}/{}/{}/".\
                    format(self.bucket,
                           self.root_path,
                           self.folder_path
                           )
        return luigi.contrib.s3.S3Target(path=output_path+"metadatos_pruebas_unitarias_FE.csv")



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
       hyper_params_grid= {'n_estimators': [self.n_estimators],
                          'max_depth': [self.max_depth],
                          'max_features': [self.max_features],
                          'min_samples_split': [self.min_samples_split],
                          'min_samples_leaf': [self.min_samples_leaf]}

       print('***** Comienza a calcular el modelo *****')
       #Se corre el modelo
       [metadata, grid_search] = funciones_mod.magic_loop_ramdomF(X_train_input, y_train, hyper_params_grid)

       self.fname = "_n_estimators_" + str(self.n_estimators) + "_max_depth_" + str(self.max_depth) + "_max_features_" + str(self.max_features) + "_min_samples_split_" + str(self.min_samples_split) + "_min_samples_leaf_" + str(self.min_samples_leaf)
       metadata = funciones_mod.completa_metadatos_modelo(metadata, self.fname)

       #Se guardan los archivos
       with self.output().open('w') as outfile1:
           metadata.to_csv(outfile1, sep='\t', encoding='utf-8', index=None, header=False)

       with open('random_forest'+self.fname+'.pkl', 'wb') as outfile2:
           pickle.dump(grid_search, outfile2)

       funciones_s3.upload_file('random_forest'+self.fname+'.pkl', self.bucket,
                                '{}/{}/random_forest{}'.format(self.root_path, self.folder_path, self.fname+'.pkl'))

    def output(self):
       output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path,
                            )

       return luigi.contrib.s3.S3Target(path=output_path+'metadata_ranfom_forest'+self.fname+'.csv')






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

       self.fname = "_penalty_" + str(self.penalty) + "_C_" + str(self.c_param)
       metadata = funciones_mod.completa_metadatos_modelo(metadata, self.fname)

       #Se guardan los archivos
       with self.output().open('w') as outfile1:
           metadata.to_csv(outfile1, sep='\t', encoding='utf-8', index=None, header=False)

       with open('reg_log'+self.fname+'.pkl', 'wb') as outfile2:
           pickle.dump(grid_search, outfile2)

       funciones_s3.upload_file('reg_log'+self.fname+'.pkl', self.bucket,
                                '{}/{}/reg_log{}'.format(self.root_path, self.folder_path, self.fname+'.pkl'))

    def output(self):
       output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path,
                            )

       return luigi.contrib.s3.S3Target(path=output_path+'metadata_reg_log'+self.fname+'.csv')




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
       hyper_params_grid= {'n_estimators': [self.n_estimators],'learning_rate':[self.learning_rate],
                          'subsample':[self.subsample], 'max_depth':[self.max_depth]}

       print('***** Comienza a calcular el modelo *****')
       #Se corre el modelo
       [metadata, grid_search] = funciones_mod.magic_loop_XGB(X_train_input, y_train, hyper_params_grid)

       self.fname = "_n_estimators_" + str(self.n_estimators) + "_learning_rate_" + str(self.learning_rate) + "_subsample_" + str(self.subsample) + "_max_depth_" + str(self.max_depth)
       metadata = funciones_mod.completa_metadatos_modelo(metadata, self.fname)

       #Se guardan los archivos
       with self.output().open('w') as outfile1:
           metadata.to_csv(outfile1, sep='\t', encoding='utf-8', index=None, header=False)

       with open('xgb'+self.fname+'.pkl', 'wb') as outfile2:
           pickle.dump(grid_search, outfile2)

       funciones_s3.upload_file('xgb'+self.fname+'.pkl', self.bucket,
                                '{}/{}/xgb{}'.format(self.root_path, self.folder_path, self.fname+'.pkl'))


    def output(self):
       output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path,
                            )

       return luigi.contrib.s3.S3Target(path=output_path+'metadata_xgb'+self.fname+'.csv')





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
             ("rank_test_score", "INT"),
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
             ("std_score_time", "VARCHAR"),
             ("params", "VARCHAR"),
             ("mean_score_time", "VARCHAR"),
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
             ("rank_test_score", "INT"),
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
             ("rank_test_score", "INT"),
             ("modelo", "VARCHAR")]

    def rows(self):
         #Leemos el df de metadatos
         with self.input()['infile2'].open('r') as infile:
              for line in infile:
                  yield line.strip("\n").split("\t")


    def requires(self):
        return  {'infile1' : CreaTablaModeloMetadatos(self.db_instance_id, self.subnet_group, self.security_group, self.host,
                                          self.database, self.user, self.password),
                 'infile2' : ModeloXGB(self.n_estimators, self.learning_rate, self.subsample, self.max_depth)}






class CorreModelos(luigi.task.WrapperTask):
    """
    Esta tarrea corre 3 modelos (Random Forest, Regresion Logistica, XGBoost) con diferentes parametros
    """
    #Parametros del modelo random forest
    n_estimators_rf = [100, 500] #luigi.IntParameter()
    max_depth_rf = [20, 50, 100] #luigi.IntParameter()
    max_features = ["sqrt", "log2"] #luigi.Parameter()
    min_samples_split = [10, 25, 50] #luigi.IntParameter()
    min_samples_leaf = [5, 10, 20]
    #Regresion logistica
    penalty = ['l1', 'l2']
    c_param = [1, 1.5]
    #XGBoost
    n_estimators = [100, 500] #luigi.IntParameter()
    learning_rate = [0.25, 0.5] #luigi.FloatParameter()
    subsample = [1]  #luigi.FloatParameter()
    max_depth = [20, 50, 100] #luigi.IntParameter()


    def requires(self):
        yield [[[[[InsertaMetadatosRandomForest(i,j,k,l,m) for i in self.n_estimators_rf] for j in self.max_depth_rf] for k in self.max_features] for l in self.min_samples_split] for m in self.min_samples_leaf]
#        yield [[InsertaMetadatosRegresion(i,j) for i in self.penalty] for j in self.c_param]
#        yield [[[[InsertaMetadatosXGB(i,j,k,l) for i in self.n_estimators] for j in self.learning_rate] for k in self.subsample] for l in self.max_depth]









class PrediccionesConMejorModelo(luigi.Task):
    #Parámetros de los modelos
    #random forest
    n_estimators_rf = luigi.IntParameter()
    max_depth_rf = luigi.IntParameter()
    max_features = luigi.Parameter()
    min_samples_split = luigi.IntParameter()
    min_samples_leaf = luigi.IntParameter()
    #Regresion logistica
    penalty = luigi.Parameter()
    c_param = luigi.IntParameter()
    #XGBooster
    n_estimators=luigi.IntParameter()
    learning_rate=luigi.IntParameter()
    subsample=luigi.IntParameter()
    max_depth=luigi.IntParameter()


    #Parámetros para la rds
    db_instance_id = 'db-dpa20'
    db_name = 'db_incidentes_cdmx'
    db_user_name = 'postgres'
    db_user_password = 'passwordDB'
    subnet_group = 'subnet_gp_dpa20'
    security_group = 'sg-09b7d6fd6a0daf19a'
    host = funciones_rds.db_endpoint(db_instance_id)

    # Parametros del Bucket
    bucket = 'dpa20-incidentes-cdmx'  #luigi.Parameter()
    root_path = 'bucket_incidentes_cdmx'
    folder_path = '5.modelo'

    #Folder para guardar la tarea actual en el s3
    folder_path_predicciones = '6.predicciones'

    def requires(self):
        return {'infiles': DummiesBase(self.db_instance_id, self.db_name, self.db_user_name,
                                      self.db_user_password, self.subnet_group, self.security_group,
                                      self.bucket, self.root_path),
                'infile1' : InsertaMetadatosXGB(self.n_estimators, self.learning_rate, self.subsample, self.max_depth),
                'infile2' : InsertaMetadatosRegresion(self.penalty, self.c_param),
                'infile3' : InsertaMetadatosRandomForest(self.n_estimators_rf, self.max_depth_rf, self.max_features,self.min_samples_split, self.min_samples_leaf)}


    #connection=pg.connect(host=db_instance_endpoint,
                     #port=port,
                     #user=DB_USER_NAME,
                     #password=DB_USER_PASSWORD,
                     #database=DB_NAME)
    def run(self):

        with self.input()['infiles']['X_train'].open('r') as infile1:
            X_train_input = pd.read_csv(infile1, sep="\t")
        with self.input()['infiles']['X_test'].open('r') as infile2:
            X_test_input = pd.read_csv(infile2, sep="\t")
        with self.input()['infiles']['y_train'].open('r') as infile3:
             y_train = pd.read_csv(infile3, sep="\t")
        with self.input()['infiles']['y_test'].open('r') as infile4:
             y_test = pd.read_csv(infile4, sep="\t")

        #hacemos la consulta para traer el nombre del archivo pickle del mejor modelo
        connection=funciones_rds.connect(self.db_name, self.db_user_name, self.db_user_password, self.host)
        archivo_mejormodelo = psql.read_sql("SELECT archivo_modelo FROM modelo.Metadatos order by cast(mean_test_score as double) DESC limit 1", connection)

        #lo extraemos del s3
        ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
        s3_resource = boto3.client('s3')
        #response=s3_resource.get_object(Bucket=bucket,Key='bucket_incidentes_cdmx/5.modelo/'+mejormodelo)
        response=s3_resource.get_object(Bucket=bucket,Key=root_path +'/'+ folder_path +'/'+ archivo_mejormodelo)
        body=response['Body'].read()
        mejor_modelo=pickle.loads(body)

        #hacemos las predicciones de la etiqueta y de las probabilidades
        mejor_modelo.fit(X_train_input,y_train)
        ynew_proba = mejor_modelo.predict_proba(X_test_input)
        ynew_etiqueta = mejor_modelo.predict(X_test_input)

        #armamos un data frame
        variables={'ynew_proba_0':ynew_proba[:,0],'ynew_proba_1':ynew_proba[:,1],'ynew_etiqueta':ynew_etiqueta,'y_test':y_test}
        predicciones=pd.DataFrame(variables)

        with self.output().open('w') as outfile:
            predicciones.to_csv(outfile, sep='\t', encoding='utf-8', index=None)

    def output(self):
        output_path = "s3://{}/{}/{}/".\
                             format(self.bucket,
                             self.root_path,
                             self.folder_path_predicciones,
                            )
        return luigi.contrib.s3.S3Target(path=output_path+'predicciones.csv')

