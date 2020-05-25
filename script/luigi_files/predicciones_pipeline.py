# config: utf8
import json, os, datetime, boto3, luigi, time, pickle
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
    month = luigi.IntParameter()
    year = luigi.IntParameter()

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
        return {'records':luigi.contrib.s3.S3Target(path=output_path+"raw/records_"+str(self.month)+"_"+str(self.year)+".json"),
                'metadata':luigi.contrib.s3.S3Target(path=output_path+"raw/metadata_"+str(self.month)+"_"+str(self.year)+".json")}





class InsertaInfoMensualRaw(CopyToTable):
    "Inserta raw de los datos mensuales" 
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
    month = 4
    year = 2020

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
        return {"infile1" : ExtraeInfoMensual(self.month, self.year, self.db_instance_id,
                                              self.database, self.user, self.password,
                                              self.subnet_group, self.security_group, self.host, self.bucket),
                "infile2" : InsertaInfoMensualRaw(self.month, self.year,
                                                  self.db_instance_id, self.subnet_group, self.security_group,
                                                  self.database, self.user, self.password, self.host,
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
    month = luigi.IntParameter()
    year = luigi.IntParameter()
    
    #Para la creacion de la base
    db_instance_id =  luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    host = luigi.Parameter()
    

    table = ""
    query = """
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
            WHERE registros->>'hora_creacion' LIKE '%\:%' and registros->>'mes'=month and registros->>'ano'=year;
            """

    def requires(self):
        # Indica que se debe hacer primero las tareas especificadas aqui
        return  [CreaTablaCleanedIncidentesInfoMensual(self.db_instance_id, self.subnet_group, self.security_group, self.host, self.database, self.user, self.password), 
                 #FuncionRemovePoints(self.db_instance_id, self.subnet_group, self.security_group, self.host, self.database, self.user, self.password),
                 #FuncionUnaccent(self.db_instance_id, self.subnet_group, self.security_group, self.host, self.database, self.user, self.password),
                 #InsertaMetadatosPruebasUnitariasExtract()
                InsertaInfoMensualRaw(self.month,self.year,self.db_instance_id,self.subnet_group,self.security_group, self.database, self.user,self.password,self.host)]








