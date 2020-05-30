# config: utf8
import json, os, datetime, boto3, luigi, requests, time
import luigi.contrib.s3
from luigi.contrib.postgres import CopyToTable, PostgresQuery
from luigi.contrib import rdbms
from luigi import task
import pandas as pd
from numpy import ndarray as np
import getpass
import funciones_rds
import funciones_s3
import funciones_req
from pruebas_unitarias import TestsForExtract, TestClean, TestFeatureEngineeringMarbles


class CreaInstanciaRDS(luigi.Task):
    """ Crea la instancia en RDS cuando se tiene el Subnet Group"""
    #Prioridad de la tarea
    priority = 100

    db_instance_id = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()

    def run(self):
        exito = funciones_rds.create_db_instance(self.db_instance_id, self.db_name, self.db_user_name, 
                                         self.db_user_password, self.subnet_group, self.security_group)
        if exito ==1:
            mins=8
            for i in range(0,mins):
                time.sleep(60)
                print("***** Wait...{} min...*****".format(mins-i))

        db_endpoint = funciones_rds.db_endpoint(self.db_instance_id)

        with self.output().open('w') as outfile:
            outfile.write(str(db_endpoint))

    def output(self):
        return luigi.LocalTarget('1.ETL_CreaInstanciaRDS.txt')



class ObtieneRDSHost(luigi.Task):
    "Obtiene el endpoint(host) de la RDS creada"
    db_instance_id = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()

    def requires(self):
        return CreaInstanciaRDS(self.db_instance_id, self.db_name, self.db_user_name,
                                self.db_user_password, self.subnet_group, self.security_group)

    def run(self):
        #Lee el endpoint del archivo
        with self.input().open() as infile, self.output().open('w') as outfile:
             endpoint = infile.read()
             print("***** Endpont ready *****", endpoint)
             outfile.write("Endpoint ready")

    def output(self):
        return luigi.LocalTarget("2.ETL_RDShost.txt")



class CreaEsquemaRAW(PostgresQuery):
    "Crea el esquema RAW dentro de la base de datos"
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
    query = "DROP SCHEMA IF EXISTS raw cascade; CREATE SCHEMA raw;"

    def requires(self):
        return ObtieneRDSHost(self.db_instance_id, self.database, self.user,
                              self.password, self.subnet_group, self.security_group)






class CreaEsquemaPruebasUnitarias(PostgresQuery):
    "Crea el esquema para PRUEBAS UNITARIAS dentro de la base de datos"
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
    query = "DROP SCHEMA IF EXISTS tests cascade; CREATE SCHEMA tests;"

    def requires(self):
        return ObtieneRDSHost(self.db_instance_id, self.database, self.user,
                              self.password, self.subnet_group, self.security_group)




class CreaTablaRawJson(PostgresQuery):
    "Crea la tabla para almacenar los datos en formato JSON, dentro del esquema RAW"
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
    query = "CREATE TABLE raw.IncidentesVialesJson(registros JSON NOT NULL);"

    def requires(self):
         return CreaEsquemaRAW(self.db_instance_id, self.subnet_group, self.security_group,
                               self.host, self.database, self.user, self.password)




class CreaTablaRawMetadatos(PostgresQuery):
    "Crea la tabla de los metadatos dentro del esquema RAW"
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
            CREATE TABLE raw.Metadatos(dataset VARCHAR, timezone VARCHAR, 
                                        rows INT, refine_ano VARCHAR, 
                                        refine_mes VARCHAR, parametro_url VARCHAR, 
                                        fecha_ejecucion VARCHAR, ip_address VARCHAR, 
                                        usuario VARCHAR, nombre_archivo VARCHAR, 
                                        formato_archivo VARCHAR); 
            """

    def requires(self):
         return CreaEsquemaRAW(self.db_instance_id, self.subnet_group, self.security_group,
                               self.host, self.database, self.user, self.password)





class CreaTablaPruebasUnitariasMetadatos(PostgresQuery):
    "Crea la tabla de los metadatos para las pruebas unitarias dentro del esquema TESTS"
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
            CREATE TABLE tests.pruebas_unitarias(fecha_ejecucion VARCHAR, ip_address VARCHAR,
                                                 usuario VARCHAR, test VARCHAR,
                                                 test_status VARCHAR, level VARCHAR, error VARCHAR);
            """

    def requires(self):
         return CreaEsquemaPruebasUnitarias(self.db_instance_id, self.subnet_group, self.security_group,
                                            self.host, self.database, self.user, self.password)








class ExtraeInfoPrimeraVez(luigi.Task):
    """
    Extrae toda la informacion: desde el inicio (1-Ene-2014) hasta 2 meses antes de la fecha actual si se corre
    antes del dia 15 del mes o, hasta 1 mes antes de la fecha actual si se corre despues del dia 15
    """
    #Para la creacion de la base
    db_instance_id =  luigi.Parameter()
    db_name = luigi.Parameter()
    db_user_name = luigi.Parameter()
    db_user_password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    host = luigi.Parameter()

    #Ruta de la API
    data_url =   "https://datos.cdmx.gob.mx/api/records/1.0/download/?dataset=incidentes-viales-c5"
    meta_url =   "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5"

    #Parametros de fechas
    year = 0
    month = 0


    def requires(self):
        # Indica que se debe hacer primero las tareas especificadas aqui
        return  [CreaTablaRawJson(self.db_instance_id, self.subnet_group, self.security_group, self.host, self.db_name, self.db_user_name, self.db_user_password),
                 CreaTablaRawMetadatos(self.db_instance_id, self.subnet_group, self.security_group, self.host, self.db_name, self.db_user_name, self.db_user_password)]


    def run(self):
        #Parametros de los datos
        DATE_START = datetime.date(2014,1,1)
#        DATE_START = datetime.date(2020,2,1)
        date_today = datetime.date.today()
        day = date_today.day
        if day > 15:
            date_end = datetime.date(date_today.year, date_today.month - 1, 1)
        else:
            date_end = datetime.date(date_today.year, date_today.month - 2, 1)

        #periodo de fechas mensuales
        dates = pd.period_range(start=str(DATE_START), end=str(date_end), freq='M')

        for date in dates:
            self.year = date.year
            self.month = date.month


            #hacemos el requerimiento para un chunk del los registros
            [records, metadata] = funciones_req.peticion_api_info_mensual(self.data_url, self.meta_url, self.month, self.year)
            funciones_rds.bulk_insert_raw([(json.dumps(records[i]['fields']) , ) for i in range(0, len(records))], [funciones_req.crea_rows_para_metadata(metadata)] , self.db_name, self.db_user_name, self.db_user_password, self.host)

        #Archivo para que Luigi sepa que ya realizo la tarea
        with self.output().open('w') as out:
            out.write('Archivo ' + str(self.year) + str(self.month) + '\n')

    def output(self):
        return luigi.LocalTarget('3.ETL_InsertarDatos.txt')




class Test1ForExtract(luigi.Task):
     "Corre las pruebas unitarias para la parte de Extract"
     db_instance_id = luigi.Parameter()
     subnet_group = luigi.Parameter()
     security_group = luigi.Parameter()

     #Para conectarse a la base
     host = luigi.Parameter()
     db_name = luigi.Parameter()
     db_user_name = luigi.Parameter()
     db_user_password = luigi.Parameter()

     bucket = luigi.Parameter()
     root_path = luigi.Parameter()
     folder_path = luigi.Parameter()


     def requires(self):
        return ExtraeInfoPrimeraVez(self.db_instance_id, self.db_name, self.db_user_name,
                                    self.db_user_password, self.subnet_group, self.security_group, self.host)

     def run(self):
        prueba_extract = TestsForExtract()
        prueba_extract.test_check_num_archivos()
        metadatos = funciones_req.metadata_para_pruebas_unitarias('test_check_num_archivos', 'SUCCESS', 'extract', 'none')

        with self.output().open('w') as out_file:
             metadatos.to_csv(out_file, sep='\t', encoding='utf-8', index=None, header=False)


     def output(self):
        output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path
                           )
        return luigi.contrib.s3.S3Target(path=output_path+"metadatos_prueba1_EXTRACT.csv")


class Test2ForExtract(luigi.Task):
     "Corre las pruebas unitarias para la parte de Extract"
     db_instance_id = luigi.Parameter()
     subnet_group = luigi.Parameter()
     security_group = luigi.Parameter()

     #Para conectarse a la base
     host = luigi.Parameter()
     db_name = luigi.Parameter()
     db_user_name = luigi.Parameter()
     db_user_password = luigi.Parameter()

     bucket = luigi.Parameter()
     root_path = luigi.Parameter()
     folder_path = luigi.Parameter()


     def requires(self):
        return ExtraeInfoPrimeraVez(self.db_instance_id, self.db_name, self.db_user_name,
                                    self.db_user_password, self.subnet_group, self.security_group, self.host)

     def run(self):
        prueba_extract = TestsForExtract()
        prueba_extract.test_check_num_registros()
        metadatos = funciones_req.metadata_para_pruebas_unitarias('test_check_num_registros', 'SUCCESS', 'extract', 'none')

        with self.output().open('w') as out_file:
             metadatos.to_csv(out_file, sep='\t', encoding='utf-8', index=None, header=False)


     def output(self):
        output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path
                           )
        return luigi.contrib.s3.S3Target(path=output_path+"metadatos_prueba2_EXTRACT.csv")




class InsertaMetadatosPruebasUnitariasExtract(CopyToTable):
    "Inserta los metadatos para las pruebas unitarias en Extract" 
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
             ("level", "VARCHAR"),
             ("error", "VARCHAR")]

    def rows(self):
         #Leemos el df de metadatos
         for file in ["infile2", "infile3"]:
              with self.input()[file].open('r') as infile:
                  for line in infile:
                      yield line.strip("\n").split("\t")



    def requires(self):
        return  { "infile1": CreaTablaPruebasUnitariasMetadatos(self.db_instance_id, self.subnet_group, self.security_group, self.host,
                                                   self.database, self.user, self.password),
                  "infile2": Test1ForExtract(self.db_instance_id, self.database, self.user, self.password,
                                            self.subnet_group, self.security_group, self.host,
                                            self.bucket, self.root_path, self.folder_path),
                  "infile3": Test2ForExtract(self.db_instance_id, self.database, self.user, self.password,
                                            self.subnet_group, self.security_group, self.host,
                                            self.bucket, self.root_path, self.folder_path)}






class CreaEsquemaCLEANED(PostgresQuery):
    "Crea el esquema CLEANED dentro de la base"
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
    query = "DROP SCHEMA IF EXISTS cleaned cascade; CREATE SCHEMA cleaned;"

    def requires(self):
        return ObtieneRDSHost(self.db_instance_id, self.database, self.user,
                              self.password, self.subnet_group, self.security_group)




class CreaTablaCleanedIncidentes(PostgresQuery):

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
            CREATE TABLE cleaned.IncidentesViales(hora_creacion TIME,
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
         return CreaEsquemaCLEANED(self.db_instance_id, self.subnet_group, self.security_group,
                                   self.host, self.database, self.user, self.password)





class CreaTablaCleanedMetadatos(PostgresQuery):
    "Crea la tabla de los metadatos dentro del esquema CLEANED"
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
            CREATE TABLE cleaned.Metadatos(fecha_ejecucion VARCHAR,
                                           ip_address VARCHAR,
                                           usuario VARCHAR,
                                           id_tarea VARCHAR,
                                           estatus_tarea VARCHAR,
                                           registros_eliminados VARCHAR); 
            """

    def requires(self):
         return CreaEsquemaCLEANED(self.db_instance_id, self.subnet_group, self.security_group,
                                   self.host, self.database, self.user, self.password)





class FuncionRemovePoints(PostgresQuery):
    """
    Funciones auxiliares para la limpieza de los datos
    """
    #Para la creacion de la base
    db_instance_id = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()

    #Para conectarse a la base
    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()

    #Funcion para eliminar puntos
    table = ""
    query="""
          CREATE OR REPLACE FUNCTION remove_points
         (
            column_text text
         )
          returns text
          language sql
          as $$
          select regexp_replace(column_text, '\.|,|\/','','g');
          $$;
          """




class FuncionUnaccent(PostgresQuery):
    """
    Funciones auxiliares para la limpieza de los datos
    """
    #Para la creacion de la base
    db_instance_id = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()

    #Para conectarse a la base
    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()

    #Funcion para eliminar puntos
    table = ""
    query="CREATE EXTENSION unaccent;"






class LimpiaInfoPrimeraVez(PostgresQuery):
    """
    Limpia toda la informacion: desde el inicio (1-Ene-2014) hasta 2 meses antes de la fecha actual
    """
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
            INSERT INTO cleaned.IncidentesViales
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
            FROM raw.IncidentesVialesJson
            WHERE registros->>'hora_creacion' LIKE '%\:%';
            """

    def requires(self):
        # Indica que se debe hacer primero las tareas especificadas aqui
        return  [CreaTablaCleanedIncidentes(self.db_instance_id, self.subnet_group, self.security_group, self.host, self.database, self.user, self.password), 
                 FuncionRemovePoints(self.db_instance_id, self.subnet_group, self.security_group, self.host, self.database, self.user, self.password),
                 FuncionUnaccent(self.db_instance_id, self.subnet_group, self.security_group, self.host, self.database, self.user, self.password),
                 InsertaMetadatosPruebasUnitariasExtract()]





class InsertaMetadatosCLEANED(luigi.Task):
    "Esta funcion inserta los metadatos de CLEANED"

    db_instance_id =  luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    subnet_group = luigi.Parameter()
    security_group = luigi.Parameter()
    host = luigi.Parameter()

    def requires(self):
        return [LimpiaInfoPrimeraVez(self.db_instance_id, self.database, self.user,
                                    self.password, self.subnet_group, self.security_group, self.host),
                CreaTablaCleanedMetadatos(self.db_instance_id, self.subnet_group, self.security_group,
                                          self.host, self.database, self.user, self.password)]

    def run(self):
        task = self.task_id
        status = 'Success'
        eliminados = 'Deleted 649 rows'
        meta = funciones_req.metadata_para_cleaned(task, status, eliminados)
        print(meta)
        funciones_rds.bulk_insert_cleaned([meta], self.database, self.user, self.password, self.host)

        #Archivo para que Luigi sepa que ya realizo la tarea
        with self.output().open('w') as out:
            out.write('Metadatos de Cleaned insertados\n')

    def output(self):
        return luigi.LocalTarget('4.ETL_LimpiaBase.txt')




class Test1ForClean(luigi.Task): 
    "Corre las pruebas unitarias para la parte de Clean"
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
    folder_path = luigi.Parameter()

    def requires(self):
        return InsertaMetadatosCLEANED(self.db_instance_id, self.db_name, self.db_user_name,
                                       self.db_user_password, self.subnet_group, self.security_group, self.host)

    def run(self):
        prueba_clean_marbles = TestClean()
        prueba_clean_marbles.test_islower_w_marbles()
        metadatos=funciones_req.metadata_para_pruebas_unitarias('test_islower_w_marbles','success','clean', 'none')

        with self.output().open('w') as out_file:
            metadatos.to_csv(out_file, sep='\t', encoding='utf-8', index=None, header=False)

    def output(self):
        output_path = "s3://{}/{}/{}/".\
                    format(self.bucket,
                           self.root_path,
                           self.folder_path
                           )
        return luigi.contrib.s3.S3Target(path=output_path+"metadatos_prueba1_CLEAN.csv")





class Test2ForClean(luigi.Task): 
    "Corre las pruebas unitarias para la parte de Clean"
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
    folder_path = luigi.Parameter()

    def requires(self):
        return InsertaMetadatosCLEANED(self.db_instance_id, self.db_name, self.db_user_name,
                                       self.db_user_password, self.subnet_group, self.security_group, self.host)

    def run(self):
        prueba_clean_marbles = TestClean()
        prueba_clean_marbles.test_correct_type()
        metadatos = funciones_req.metadata_para_pruebas_unitarias('test_correct_type','success','clean', 'none')

        with self.output().open('w') as out_file:
            metadatos.to_csv(out_file, sep='\t', encoding='utf-8', index=None, header=False)

    def output(self):
        output_path = "s3://{}/{}/{}/".\
                    format(self.bucket,
                           self.root_path,
                           self.folder_path
                           )
        return luigi.contrib.s3.S3Target(path=output_path+"metadatos_prueba2_CLEAN.csv")




class InsertaMetadatosPruebasUnitariasClean(CopyToTable):
    "Inserta los metadatos para las pruebas unitarias en Clean"
    # Parametros del RDS
    db_instance_id = 'db-dpa20'
    subnet_group = 'subnet_gp_dpa20'
    security_group = 'sg-09b7d6fd6a0daf19a'

    # Para condectarse a la Base
    database = 'db_incidentes_cdmx'
    user = 'postgres'
    password = 'passwordDB'
    host = funciones_rds.db_endpoint(db_instance_id)

    #Parametros del bucket
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
             ("level", "VARCHAR"),
             ("error", "VARCHAR")]

    def rows(self):
         #Leemos el df de metadatos
         for file in ["infile1", "infile2"]:
              with self.input()[file].open('r') as infile:
                  for line in infile:
                      yield line.strip("\n").split("\t")


    def requires(self):
        return  { "infile1": Test1ForClean(self.db_instance_id, self.subnet_group, self.security_group, self.host, self.database,
                                       self.user, self.password, self.bucket, self.root_path, self.folder_path),
                  "infile2": Test2ForClean(self.db_instance_id, self.subnet_group, self.security_group, self.host, self.database,
                                       self.user, self.password, self.bucket, self.root_path, self.folder_path)}













