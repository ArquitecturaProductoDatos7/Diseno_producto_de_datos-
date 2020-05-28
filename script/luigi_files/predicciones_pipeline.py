# config: utf8
import json, os, datetime, boto3, luigi, time, pickle, socket, getpass
import luigi.contrib.s3
from luigi.contrib.postgres import CopyToTable, PostgresQuery
#from luigi.contrib import rdbms
#from luigi import task
import pandas as pd
import pandas.io.sql as psql
import funciones_rds
import funciones_s3
import funciones_req
import funciones_mod
import etl_pipeline_ver6
from etl_pipeline_ver6 import ObtieneRDSHost, InsertaMetadatosPruebasUnitariasClean, CreaEsquemaRAW
from pruebas_unitarias import TestsForExtract, TestClean, TestFeatureEngineeringMarbles
from pruebas_unitarias import TestFeatureEngineeringMarbles, TestFeatureEngineeringPandas



class CreaTablaRawInfoMensual(PostgresQuery):
    "Crea la tabla para almacenar los datos MENSUALES en formato JSON, dentro del esquema RAW"
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
    query = "CREATE TABLE raw.InfoMensual(registros JSON NOT NULL);"

    def requires(self):
         return CreaEsquemaRAW(self.db_instance_id, self.subnet_group, self.security_group,
                               self.host, self.database, self.user, self.password)







class ExtraeInfoMensual(luigi.Task):
    """
    Extrae la informacion de la API, segun el MES y ANO solicitado
    """
    #Mes a extraer
    month = luigi.Parameter()
    year = luigi.Parameter()

    #Para la base
    db_instance_id = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group =  luigi.Parameter()
    host =  luigi.Parameter()

    #Para el bucket
    bucket = luigi.Parameter()
    root_path = 'bucket_incidentes_cdmx'
    folder_path = '9.predicciones'


    #Ruta de la API
    data_url =   "https://datos.cdmx.gob.mx/api/records/1.0/download/?dataset=incidentes-viales-c5"
    meta_url =   "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5"


    def requires(self):
        return CreaTablaRawInfoMensual(self.db_instance_id, self.subnet_group, self.security_group,
                                       self.host, self.db_name, self.db_user_name, self.db_user_password)


    def run(self):
        [registros, metadata] = funciones_req.peticion_api_info_mensual(self.data_url, self.meta_url, self.month, self.year)

        with self.output()['records'].open('w') as outfile1:
             json.dump(registros, outfile1)

        with self.output()['metadata'].open('w') as outfile2:
             json.dump(metadata, outfile2)


    def output(self):
        output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path
                            )
        return {'records':luigi.contrib.s3.S3Target(path=output_path+"raw/records_"+self.month+"_"+self.year+".json"),
                'metadata':luigi.contrib.s3.S3Target(path=output_path+"raw/metadata_"+self.month+"_"+self.year+".json")}





class InsertaInfoMensualRaw(CopyToTable):
    "Inserta raw de los datos mensuales" 
    #Mes a extraer
    month = luigi.Parameter()
    year = luigi.Parameter()

    # Parametros del RDS
    db_instance_id = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    # Para condectarse a la Base
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    host = luigi.Parameter()

    bucket = luigi.Parameter()
    root_path = 'bucket_incidentes_cdmx'
    folder_path = '9.predicciones'


    # Nombre de la tabla a insertar
    table = 'raw.InfoMensual'

    # Estructura de las columnas que integran la tabla (ver esquema)
    columns=[("registros", "JSON")]

    def rows(self):
        #Leemos el df de metadatos
        with self.input()['infile1']['records'].open('r') as infile:
             records = json.load(infile)
             for i in range(0, len(records)):
                   yield [json.dumps(records[i]['fields'])]

    def requires(self):
        return {"infile1" : ExtraeInfoMensual(self.month, self.year, self.db_instance_id,
                                              self.database, self.user, self.password,
                                              self.subnet_group, self.security_group, self.host, self.bucket),
                "infile2" : CreaTablaRawInfoMensual(self.db_instance_id, self.subnet_group, self.security_group,
                                                    self.host, self.database, self.user, self.password)}





class InsertaMetadataInfoMensualRaw(CopyToTable):
    "Inserta el metadata de los datos mensuales" 

    #Mes a extraer
    month = luigi.Parameter()
    year = luigi.Parameter()

    # Parametros del RDS
    db_instance_id = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    # Para condectarse a la Base
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    host = luigi.Parameter()

    bucket = luigi.Parameter()
    root_path = 'bucket_incidentes_cdmx'
    folder_path = '9.predicciones'


   # Nombre de la tabla a insertar
    table = 'raw.Metadatos'

    # Estructura de las columnas que integran la tabla (ver esquema)
    columns=[("dataset", "VARCHAR"),
             ("timezone", "VARCHAR"),
             ("rows", "INT"),
             ("refine_ano", "VARCHAR"),
             ("refine_mes", "VARCHAR"),
             ("parametro_url", "VARCHAR"),
             ("fecha_ejecucion", "VARCHAR"),
             ("ip_address", "VARCHAR"),
             ("usuario", "VARCHAR"),
             ("nombre_archivo", "VARCHAR"),
             ("formato_archivo", "VARCHAR")]

    def rows(self):
        #Leemos el df de metadatos
        with self.input()['infile1']['metadata'].open('r') as infile:
             records = json.load(infile)
             yield [json.dumps(records[campo]) for campo in records.keys()]


    def requires(self):
        return {"infile1" : ExtraeInfoMensual(self.month, self.year, 
                                              self.db_instance_id, self.subnet_group, self.security_group,
                                              self.database, self.user, self.password, self.host,
                                              self.bucket), 
                "infile2" : InsertaInfoMensualRaw(self.month, self.year,
                                                  self.db_instance_id, self.subnet_group, self.security_group,
                                                  self.database, self.user, self.password, self.host,
                                                  self.bucket)}






class Test1ForExtractInfoMensual(luigi.Task):
     "Corre las pruebas unitarias para la parte de Extract"

     #Mes a extraer
     month = luigi.Parameter()
     year = luigi.Parameter()

     db_instance_id = luigi.Parameter()
     subnet_group = luigi.Parameter()
     security_group = luigi.Parameter()

     #Para conectarse a la base
     db_name = luigi.Parameter()
     db_user_name = luigi.Parameter()
     db_user_password = luigi.Parameter()
     host = luigi.Parameter()

     bucket = luigi.Parameter()
     root_path = 'bucket_incidentes_cdmx'
     folder_path = '0.pruebas_unitarias'

     def requires(self):
        return InsertaMetadataInfoMensualRaw(self.month, self.year,
                                             self.db_instance_id, self.subnet_group, self.security_group,
                                             self.db_name, self.db_user_name, self.db_user_password, self.host,
                                             self.bucket)

     def run(self):
        prueba_extract = TestsForExtract()
        prueba_extract.test_check_num_archivos_info_mensual()
        metadatos = funciones_req.metadata_para_pruebas_unitarias('test_check_num_archivos_info_mensual', 'SUCCESS', 'extract')

        with self.output().open('w') as out_file:
             metadatos.to_csv(out_file, sep='\t', encoding='utf-8', index=None, header=False)


     def output(self):
        output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path
                           )
        return luigi.contrib.s3.S3Target(path=output_path+"metadatos_prueba1_EXTRACT_info_mensual_mes_"+self.month+"_ano_"+self.year+".csv")





class Test2ForExtractInfoMensual(luigi.Task):
     "Corre las pruebas unitarias para la parte de Extract"

     #Mes a extraer
     month = luigi.Parameter()
     year = luigi.Parameter()

     db_instance_id = luigi.Parameter()
     subnet_group = luigi.Parameter()
     security_group = luigi.Parameter()

     #Para conectarse a la base
     db_name = luigi.Parameter()
     db_user_name = luigi.Parameter()
     db_user_password = luigi.Parameter()
     host = luigi.Parameter()

     bucket = luigi.Parameter()
     root_path = 'bucket_incidentes_cdmx'
     folder_path = '9.predicciones'


     def requires(self):
        return InsertaMetadataInfoMensualRaw(self.month, self.year,
                                             self.db_instance_id, self.subnet_group, self.security_group,
                                             self.db_name, self.db_user_name, self.db_user_password, self.host,
                                             self.bucket)

     def run(self):
        prueba_extract = TestsForExtract()
        prueba_extract.test_check_num_registros_info_mensual()
        metadatos = funciones_req.metadata_para_pruebas_unitarias('test_check_num_registros_info_mensual', 'SUCCESS', 'extract')

        with self.output().open('w') as out_file:
             metadatos.to_csv(out_file, sep='\t', encoding='utf-8', index=None, header=False)


     def output(self):
        output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path
                           )
        return luigi.contrib.s3.S3Target(path=output_path+"metadatos_prueba2_EXTRACT_info_mensual_mes_"+self.month+"_ano_"+self.year+".csv")






class InsertaMetadatosPruebasUnitariasExtractInfoMensual(CopyToTable):
    "Inserta los metadatos para las pruebas unitarias en Extract" 
    #Mes a extraer
    month = luigi.Parameter()
    year = luigi.Parameter()

    db_instance_id = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()

    #Para conectarse a la base
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    host = luigi.Parameter()

    bucket = luigi.Parameter()

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
         #Leemos el df de metadatos
         for file in ["infile1", "infile2"]:
              with self.input()[file].open('r') as infile:
                  for line in infile:
                      yield line.strip("\n").split("\t")



    def requires(self):
        return  { "infile1": Test1ForExtractInfoMensual(self.month, self.year,
                                             self.db_instance_id, self.subnet_group, self.security_group,
                                             self.database, self.user, self.password,  self.host,
                                             self.bucket),
                  "infile2": Test2ForExtractInfoMensual(self.month, self.year,
                                             self.db_instance_id, self.subnet_group, self.security_group,
                                             self.database, self.user, self.password,  self.host,
                                             self.bucket)}







class CreaTablaCleanedIncidentesInfoMensual(PostgresQuery):

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
            CREATE TABLE cleaned.IncidentesVialesInfoMensual(hora_creacion TIME,
                                                  delegacion_inicio VARCHAR,
                                                  dia_semana VARCHAR,
                                                  tipo_entrada VARCHAR,
                                                  mes SMALLINT,
                                                  latitud FLOAT,
                                                  longitud FLOAT,
                                                  ano INT,
                                                  incidente_c4 VARCHAR,
                                                  codigo_cierre VARCHAR);
            """

    def requires(self):
         return etl_pipeline_ver6.CreaEsquemaCLEANED(self.db_instance_id, self.subnet_group, self.security_group,
                                   self.host, self.database, self.user, self.password)
        
        

class LimpiaInfoMensual(PostgresQuery):
    """
    Limpia la información de los meses nuevos
    """
    
    #Mes a extraer
    month = luigi.Parameter()
    year = luigi.Parameter()

    # Parametros del RDS
    db_instance_id = luigi.Parameter()
    # Para condectarse a la Base
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    host = luigi.Parameter()

    bucket = luigi.Parameter()

    #clase = InsertaMetadataInfoMensualRaw()
    #mes = getattr(clase,'month')
    #ano = getattr(clase,'year')

    table=''
    query =""

    def run(self):
        connection = self.output().connect()
        connection.autocommit = self.autocommit
        cursor = connection.cursor()
        sql="""
            INSERT INTO cleaned.IncidentesVialesInfoMensual
            SELECT (registros->>'hora_creacion')::TIME,
                   remove_points(LOWER(registros->>'delegacion_inicio')),
                   unaccent(LOWER(registros->>'dia_semana')),
                   unaccent(LOWER(registros->>'tipo_entrada')),
                   (registros->>'mes')::smallint,
                   (registros->>'latitud')::float,
                   (registros->>'longitud')::float,
                   (registros->>'ano')::int,
                   unaccent(remove_points(LOWER(registros->>'incidente_c4'))),
                   unaccent(remove_points(LOWER(registros->>'codigo_cierre')))
            FROM raw.InfoMensual
            WHERE registros->>'hora_creacion' LIKE '%\:%' and registros->>'mes'=\'{}\' and registros->>'ano'=\'{}\'
            """.format(self.month, self.year)

        #logger.info('Executing query from task: {name}'.format(name=self.__class__))
        cursor.execute(sql)

        # Update marker table
        self.output().touch(connection)

        # commit and close connection
        connection.commit()
        connection.close()

    def requires(self):
        return  [CreaTablaCleanedIncidentesInfoMensual(self.db_instance_id, self.subnet_group, self.security_group,
                                                       self.host, self.database, self.user, self.password),
                 InsertaMetadatosPruebasUnitariasExtractInfoMensual(self.month, self.year,
                                                            self.db_instance_id, self.subnet_group, self.security_group,
                                                            self.database, self.user, self.password, self.host, self.bucket)]

    
class InsertaMetadatosCLEANEDInfoMensual(CopyToTable):
    "Esta funcion inserta los metadatos de CLEANED"

    #Mes a extraer
    month = luigi.Parameter()
    year = luigi.Parameter()
    
    # Parametros del RDS
    db_instance_id = luigi.Parameter()
    # Para condectarse a la Base
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    host = luigi.Parameter()

    bucket = luigi.Parameter()

    def requires(self):
        return LimpiaInfoMensual(self.month, self.year, self.db_instance_id, self.database, self.user,
                                    self.password, self.subnet_group, self.security_group, self.host, self.bucket)

    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    date_time = datetime.datetime.now()
    task = 'InsertaMetadatosCLEANEDInfoMensual'

    fecha_de_ejecucion = date_time.strftime("%d/%m/%Y %H:%M:%S")
    ip_address = ip_address
    usuario = getpass.getuser()
    task_id = task
    task_status = 'Success'
    registros_eliminados = 'Deleted 0 rows'

    table = "cleaned.Metadatos"

    columns=[("fecha_ejecucion", "VARCHAR"),
             ("ip_address", "VARCHAR"),
             ("usuario", "VARCHAR"),
             ("id_tarea", "VARCHAR"),
             ("estatus_tarea", "VARCHAR"),
             ("registros_eliminados", "VARCHAR")]


    def rows(self):
        r=[(self.fecha_de_ejecucion,self.ip_address,self.usuario,self.task_id,self.task_status,self.registros_eliminados)]
        return(r)


class Test1ForCleanInfoMensual(luigi.Task): 
    "Corre las pruebas unitarias para la parte de Clean"
    
    #Mes a extraer
    month = luigi.Parameter()
    year = luigi.Parameter()
    
    #Parametros
    db_instance_id = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    host = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()

    bucket = luigi.Parameter()
    root_path = luigi.Parameter()
    folder_path = '0.pruebas_unitarias'

    def requires(self):
        return InsertaMetadatosCLEANEDInfoMensual(self.month, self.year, self.db_instance_id, self.db_name, self.db_user_name,
                                                  self.db_user_password, self.subnet_group, self.security_group, self.host, self.bucket)

    def run(self):
        prueba_clean_marbles = TestClean()
        prueba_clean_marbles.test_islower_w_marbles_info_mensual()
        #metadatos=funciones_req.metadata_para_pruebas_unitarias('test_islower_w_marbles','success','clean')
        metadatos=funciones_req.metadata_para_pruebas_unitarias('test_islower_w_marbles_info_mensual','success','clean')

        with self.output().open('w') as out_file:
            metadatos.to_csv(out_file, sep='\t', encoding='utf-8', index=None, header=False)

    def output(self):
        output_path = "s3://{}/{}/{}/".\
                    format(self.bucket,
                           self.root_path,
                           self.folder_path
                           )
        return luigi.contrib.s3.S3Target(path=output_path+"metadatos_prueba1_CLEAN_info_mensual_mes_"+self.month+"_ano_"+self.year+".csv")






class Test2ForCleanInfoMensual(luigi.Task): 
    "Corre las pruebas unitarias para la parte de Clean"

    #Mes a extraer
    month = luigi.Parameter()
    year = luigi.Parameter()

    #Parametros
    db_instance_id = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    host = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()

    bucket = luigi.Parameter()
    root_path = luigi.Parameter()
    folder_path = '0.pruebas_unitarias'

    def requires(self):
        return InsertaMetadatosCLEANEDInfoMensual(self.month, self.year, self.db_instance_id, self.db_name,
                                                  self.db_user_name, self.db_user_password, self.subnet_group, self.security_group, self.host,
                                                  self.bucket)

    def run(self):
        prueba_clean_marbles = TestClean()
        prueba_clean_marbles.test_correct_type_info_mensual()
        #metadatos = funciones_req.metadata_para_pruebas_unitarias('test_correct_type','success','clean')
        metadatos = funciones_req.metadata_para_pruebas_unitarias('test_correct_type_info_mensual','success','clean')

        with self.output().open('w') as out_file:
            metadatos.to_csv(out_file, sep='\t', encoding='utf-8', index=None, header=False)

    def output(self):
        output_path = "s3://{}/{}/{}/".\
                    format(self.bucket,
                           self.root_path,
                           self.folder_path
                           )
        return luigi.contrib.s3.S3Target(path=output_path+"metadatos_prueba2_CLEAN_info_mensual_mes_"+self.month+"_ano_"+self.year+".csv")




class InsertaMetadatosPruebasUnitariasCleanInfoMensual(CopyToTable):
    "Inserta los metadatos para las pruebas unitarias en Clean"

    #Mes a extraer
    month = luigi.IntParameter()
    year = luigi.IntParameter()

    # Parametros del RDS
    db_instance_id = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()

    # Para condectarse a la Base
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    host = luigi.Parameter()

    bucket = luigi.Parameter()
    root_path = luigi.Parameter()

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
        #Leemos el df de metadatos
        for file in ["infile1", "infile2"]:
            with self.input()[file].open('r') as infile:
                for line in infile:
                    yield line.strip("\n").split("\t")


    def requires(self):
        return  { "infile1": Test1ForCleanInfoMensual(self.month, self.year, self.db_instance_id, self.subnet_group,
                                                      self.security_group, self.host, self.database, self.user, self.password, self.bucket, self.root_path),
                  "infile2": Test2ForCleanInfoMensual(self.month, self.year, self.db_instance_id, self.subnet_group, self.security_group, self.host, self.database, self.user, self.password, self.bucket, self.root_path)}





class PreprocesoBaseInfoMensual(luigi.Task):
    """  """
    #Mes a extraer
    month = "4" #luigi.IntParameter()
    year = "2020" #luigi.IntParameter()

    # Parametros del RDS
    db_instance_id = 'db-dpa20'  #luigi.Parameter()
    subnet_group = 'subnet_gp_dpa20' # luigi.Parameter()
    security_group = 'sg-09b7d6fd6a0daf19a' # luigi.Parameter()
    # Para condectarse a la Base
    database =  'db_incidentes_cdmx' # luigi.Parameter()
    user =  'postgres' #luigi.Parameter()
    password = 'passwordDB' #luigi.Parameter()
    host = funciones_rds.db_endpoint(db_instance_id)  #luigi.Parameter()
    #Parametros del bucket
    bucket = 'dpa20-incidentes-cdmx'  #luigi.Parameter()
    root_path = 'bucket_incidentes_cdmx'  #luigi.Parameter()

    pre_path = '1.preprocesamiento'
    file_name = 'base_procesada_info_mensual'

    def requires(self):
       return InsertaMetadatosPruebasUnitariasCleanInfoMensual(self.month, self.year,
                                                               self.db_instance_id, self.subnet_group, self.security_group,
                                                               self.database, self.user, self.password, self.host,
                                                               self.bucket, self.root_path)

    def run(self):
       dataframe = funciones_rds.obtiene_df(self.database, self.user, self.password, self.host, "incidentesvialesinfomensual" , "cleaned")
       dataframe = funciones_mod.preprocesamiento_variable(dataframe)
       dataframe = funciones_mod.elimina_na_de_variable_delegacion(dataframe)

#       ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
#       s3_resource = ses.resource('s3')
#       obj = s3_resource.Bucket(self.bucket)

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


