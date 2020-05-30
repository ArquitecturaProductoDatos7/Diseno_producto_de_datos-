# config: utf8
import json, os, datetime, boto3, luigi, time, pickle, socket, getpass
import luigi.contrib.s3
from luigi.contrib.postgres import CopyToTable, PostgresQuery
from luigi import Event
import pandas as pd
import pandas.io.sql as psql
import funciones_rds
import funciones_s3
import funciones_req
import funciones_mod
import etl_pipeline_ver6
from etl_pipeline_ver6 import ObtieneRDSHost, InsertaMetadatosPruebasUnitariasClean, CreaEsquemaRAW
from modelado_pipeline import SeparaBase, SeleccionaMejorModelo, CreaEsquemaPrediccion
from pruebas_unitarias import TestsForExtract, TestClean, TestFeatureEngineeringMarbles
from pruebas_unitarias import TestFeatureEngineeringMarbles, TestFeatureEngineeringPandas, TestsForPredicciones



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
    subnet_group = luigi.Parameter()
    security_group =  luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    host =  luigi.Parameter()

    #Para el bucket
    bucket = luigi.Parameter()
    root_path = luigi.Parameter()

    folder_path = '9.predicciones'


    #Ruta de la API
    data_url =   "https://datos.cdmx.gob.mx/api/records/1.0/download/?dataset=incidentes-viales-c5"
    meta_url =   "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5"


    def requires(self):
        return CreaTablaRawInfoMensual(self.db_instance_id, self.subnet_group, self.security_group,
                                       self.host, self.database, self.user, self.password)


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
    root_path = luigi.Parameter()

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
        return {"infile1" : ExtraeInfoMensual(self.month, self.year, 
                                              self.db_instance_id, self.subnet_group, self.security_group,
                                              self.database, self.user, self.password, self.host, 
                                              self.bucket, self.root_path),
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
    root_path = luigi.Parameter()

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
                                              self.bucket, self.root_path),
                "infile2" : InsertaInfoMensualRaw(self.month, self.year,
                                                  self.db_instance_id, self.subnet_group, self.security_group,
                                                  self.database, self.user, self.password, self.host,
                                                  self.bucket, self.root_path)}






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
     root_path = luigi.Parameter()

     folder_path = '0.pruebas_unitarias'

     def requires(self):
        return InsertaMetadataInfoMensualRaw(self.month, self.year,
                                             self.db_instance_id, self.subnet_group, self.security_group,
                                             self.db_name, self.db_user_name, self.db_user_password, self.host,
                                             self.bucket, self.root_path)

     def run(self):
        prueba_extract = TestsForExtract()
        prueba_extract.test_check_num_archivos_info_mensual()
        metadatos = funciones_req.metadata_para_pruebas_unitarias('test_check_num_archivos_info_mensual', 'SUCCESS', 'extract','none')

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
     root_path = luigi.Parameter()

     folder_path = '9.predicciones'


     def requires(self):
        return InsertaMetadataInfoMensualRaw(self.month, self.year,
                                             self.db_instance_id, self.subnet_group, self.security_group,
                                             self.db_name, self.db_user_name, self.db_user_password, self.host,
                                             self.bucket, self.root_path)

     def run(self):
        prueba_extract = TestsForExtract()
        prueba_extract.test_check_num_registros_info_mensual()
        metadatos = funciones_req.metadata_para_pruebas_unitarias('test_check_num_registros_info_mensual', 'SUCCESS', 'extract','none')

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
    root_path = luigi.Parameter()

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
        return  { "infile1": Test1ForExtractInfoMensual(self.month, self.year,
                                             self.db_instance_id, self.subnet_group, self.security_group,
                                             self.database, self.user, self.password,  self.host,
                                             self.bucket, self.root_path),
                  "infile2": Test2ForExtractInfoMensual(self.month, self.year,
                                             self.db_instance_id, self.subnet_group, self.security_group,
                                             self.database, self.user, self.password,  self.host,
                                             self.bucket, self.root_path)}







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
    root_path = luigi.Parameter()

    table = ""
    query = ""

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
                                                                    self.database, self.user, self.password, self.host,
                                                                    self.bucket, self.root_path)]




class InsertaMetadatosCLEANEDInfoMensual(CopyToTable):
    "Esta funcion inserta los metadatos de CLEANED"

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
    root_path = luigi.Parameter()

    def requires(self):
        return LimpiaInfoMensual(self.month, self.year,
                                 self.db_instance_id, self.database, self.user,
                                 self.password, self.subnet_group, self.security_group, self.host,
                                 self.bucket, self.root_path)

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
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    host = luigi.Parameter()

    bucket = luigi.Parameter()
    root_path = luigi.Parameter()
    folder_path = '0.pruebas_unitarias'

    def requires(self):
        return InsertaMetadatosCLEANEDInfoMensual(self.month, self.year,
                                                  self.db_instance_id, self.subnet_group, self.security_group,
                                                  self.database, self.user, self.password, self.host,
                                                  self.bucket, self.root_path)

    def run(self):
        prueba_clean_marbles = TestClean()
        prueba_clean_marbles.test_islower_w_marbles_info_mensual()
        metadatos=funciones_req.metadata_para_pruebas_unitarias('test_islower_w_marbles_info_mensual','success','clean','none')

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
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    host = luigi.Parameter()

    bucket = luigi.Parameter()
    root_path = luigi.Parameter()
    folder_path = '0.pruebas_unitarias'

    def requires(self):
        return InsertaMetadatosCLEANEDInfoMensual(self.month, self.year,
                                                  self.db_instance_id, self.subnet_group, self.security_group,
                                                  self.database, self.user, self.password, self.host,
                                                  self.bucket, self.root_path)


    def run(self):
        prueba_clean_marbles = TestClean()
        prueba_clean_marbles.test_correct_type_info_mensual()
        #metadatos = funciones_req.metadata_para_pruebas_unitarias('test_correct_type','success','clean')
        metadatos = funciones_req.metadata_para_pruebas_unitarias('test_correct_type_info_mensual','success','clean','none')

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
        return  { "infile1": Test1ForCleanInfoMensual(self.month, self.year,
                                                      self.db_instance_id, self.subnet_group, self.security_group,
                                                      self.database, self.user, self.password, self.host,
                                                      self.bucket, self.root_path),
                  "infile2": Test2ForCleanInfoMensual(self.month, self.year,
                                                      self.db_instance_id, self.subnet_group, self.security_group,
                                                      self.database, self.user, self.password, self.host,
                                                      self.bucket, self.root_path)}





class PreprocesoBaseInfoMensual(luigi.Task):
    """ Preprocesa la Informacion Mensual """
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
    #Parametros del bucket
    bucket = luigi.Parameter()
    root_path = luigi.Parameter()

    folder_path = '1.preprocesamiento'

    def requires(self):
       return InsertaMetadatosPruebasUnitariasCleanInfoMensual(self.month, self.year,
                                                               self.db_instance_id, self.subnet_group, self.security_group,
                                                               self.database, self.user, self.password, self.host,
                                                               self.bucket, self.root_path)

    def run(self):
       dataframe = funciones_rds.obtiene_df(self.database, self.user, self.password, self.host, "incidentesvialesinfomensual" , "cleaned")
       dataframe = funciones_mod.preprocesamiento_variable(dataframe)
       dataframe = funciones_mod.elimina_na_de_variable_delegacion(dataframe)
       #selecciono vars para el modelo
       vars_modelo = ['delegacion_inicio','mes','dia_semana','hora', 'tipo_entrada', 'incidente_c4_rec']
       dataframe = dataframe[vars_modelo]

       with self.output().open('w') as out_file:
            dataframe.to_csv(out_file, sep='\t', encoding='utf-8', index=None)


    def output(self):
       output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path
                           )
       return luigi.contrib.s3.S3Target(path=output_path+"X_info_mensual_mes_"+self.month+"_ano_"+self.year+".csv")






class ImputacionesBaseInfoMensual(luigi.Task):
    "Esta tarea hace la imputacion de la base en la Train & Test"

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
    #Parametros del bucket
    bucket = luigi.Parameter()
    root_path = luigi.Parameter()

    #Para la tarea actual
    folder_path = '3.imputaciones'

    def requires(self):
       return {'infiles1' : PreprocesoBaseInfoMensual(self.month, self.year,
                                                      self.db_instance_id, self.subnet_group, self.security_group,
                                                      self.database, self.user, self.password, self.host,
                                                      self.bucket, self.root_path),
               'infiles2' : SeparaBase(self.db_instance_id, self.database, self.user,
                                       self.password, self.subnet_group, self.security_group,
                                       self.bucket, self.root_path)}

    def run(self):
       #Se abren los archivos
       with self.input()['infiles1'].open('r') as infile4:
             X_info_mensual = pd.read_csv(infile4, sep="\t")
       with self.input()['infiles2']['X_train'].open('r') as infile1:
             X_train_old = pd.read_csv(infile1, sep="\t")

       #Se realizan las imputaciones
       [X_train_old, X_info_mensual] = funciones_mod.imputacion_variable_delegacion(X_train_old, X_info_mensual)

       #Se guardan el archivos con la Info Mensual imputada
       with self.output().open('w') as outfile1:
            X_info_mensual.to_csv(outfile1, sep='\t', encoding='utf-8', index=None)



    def output(self):
       output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path,
                            )
       return luigi.contrib.s3.S3Target(path=output_path+'X_info_mensual_mes_'+self.month+'_ano_'+self.year+'.csv')





class DummiesBaseInfoMensual(luigi.Task):
    "Esta tarea convierte las variables categoricas a dummies (One-hot encoder) para la base Train & Test"

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
    #Parametros del bucket
    bucket = luigi.Parameter()
    root_path = luigi.Parameter()

    #Para la tarea actual
    folder_path = '4.input_modelo'

    def requires(self):
       return {'infiles' : ImputacionesBaseInfoMensual(self.month, self.year,
                                                        self.db_instance_id, self.subnet_group, self.security_group,
                                                        self.database, self.user, self.password, self.host,
                                                        self.bucket, self.root_path)}

    def run(self):
       #Se abre el archivo con la info mensual
       with self.input()['infiles'].open('r') as infile1:
             X_info_mensual = pd.read_csv(infile1, sep="\t")

       #Se hace el one-hot encoder y se obtiene la base lista para el modelo
       [X_info_mensual, X_info_mensual] = funciones_mod.dummies_para_categoricas(X_info_mensual, X_info_mensual)

       #Se guarda el archivo
       with self.output().open('w') as outfile1:
            X_info_mensual.to_csv(outfile1, sep='\t', encoding='utf-8', index=None)


    def output(self):
       output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path,
                            )
       return luigi.contrib.s3.S3Target(path=output_path+'X_info_mensual_mes_'+self.month+'_ano_'+self.year+'.csv')





class InsertaMetadatosFeatuEnginInfoMensual(CopyToTable):
    """
    Esta funcion inserta los metadatos de Feature Engineering
    """
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
    #Parametros del bucket
    bucket = luigi.Parameter()
    root_path = luigi.Parameter()

    #Data
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    date_time = datetime.datetime.now()
    task = 'InsertaMetadatosFeatuEnginInfoMensual'

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
        return DummiesBaseInfoMensual(self.month, self.year,
                                      self.db_instance_id, self.subnet_group, self.security_group,
                                      self.database, self.user, self.password, self.host,
                                      self.bucket, self.root_path)



# PRUEBAS UNITARAS DE FE (maggie)

# METADATSO DE PRUEBAS UNITARIAS (maggie)


# LUIGI TASK PARA ANADIR LAS COLUMNAS QUE FALTAN (Bren)
class InsertaColumnasInfoMensual(luigi.Task):
    """Esta tarea inserta las columnas faltantes al df de Info Mensual para el modelo"""
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
    #Parametros del bucket
    bucket = luigi.Parameter()
    root_path = luigi.Parameter()

    #Para la tarea actual
    folder_path = '4.input_modelo'


    def requires(self):
        return {"infiles1" : DummiesBaseInfoMensual(self.month, self.year,
                                                    self.db_instance_id, self.subnet_group, self.security_group,
                                                    self.database, self.user, self.password, self.host,
                                                    self.bucket, self.root_path),
                "infiles2" : InsertaMetadatosFeatuEnginInfoMensual(self.month, self.year,
                                                                   self.db_instance_id, self.subnet_group, self.security_group,
                                                                   self.database, self.user, self.password, self.host,
                                                                   self.bucket, self.root_path)}



    def run(self):
        with self.input()['infiles1'].open('r') as infile1:
            X_info_mensual = pd.read_csv(infile1, sep="\t")
        #Se anaden las columnas
        X_info_mensual.insert(28, "tipo_entrada_llamada del 066", 0)
        X_info_mensual.insert(32, "tipo_entrada_zello", 0)

        #Se guarda el archivo
        with self.output().open('w') as outfile1:
            X_info_mensual.to_csv(outfile1, sep='\t', encoding='utf-8', index=None)


    def output(self):
       output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path,
                            )
       return luigi.contrib.s3.S3Target(path=output_path+'X_input_info_mensual_mes_'+self.month+'_ano_'+self.year+'.csv')




class PrediccionesInfoMensual(luigi.Task):
    """ Calcula las predicciones para la Info Mensual"""
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
    #Parametros del bucket
    bucket = luigi.Parameter()
    root_path = luigi.Parameter()

    #Folder para guardar la tarea actual en el s3
    folder_path = '9.predicciones'

    def requires(self):
        return {"infile1" : InsertaColumnasInfoMensual(self.month, self.year,
                                                       self.db_instance_id, self.subnet_group, self.security_group,
                                                       self.database, self.user, self.password, self.host,
                                                       self.bucket, self.root_path)}


    def run(self):
        with self.input()['infile1'].open('r') as infile1:
            X_info_mensual = pd.read_csv(infile1, sep="\t")

        #extraemos el modelo del s3
        ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
        s3_resource = boto3.client('s3')
        response = s3_resource.list_objects(Bucket=self.bucket, Prefix='{}/{}/'.format(self.root_path, '7.modelo_final'))
        for file in response['Contents']:
            fname = file['Key']
            if fname.endswith('.pkl'):
               path_to_file = fname

        response = s3_resource.get_object(Bucket=self.bucket, Key=path_to_file)
        body = response['Body'].read()
        mejor_modelo = pickle.loads(body)

        #hacemos las predicciones de la etiqueta y de las probabilidades
        ynew_proba = mejor_modelo.predict_proba(X_info_mensual)
        ynew_etiqueta = mejor_modelo.predict(X_info_mensual)

        metadata = funciones_mod.metadata_predicciones(self.month, self.year, path_to_file.split('/',3)[2])

        #df para predicciones
        df_aux = funciones_mod.hace_df_para_ys(ynew_proba, ynew_etiqueta, pd.Series(ynew_etiqueta))
        x_test_sin_dummies = funciones_mod.dummies_a_var_categorica(X_info_mensual, ['delegacion_inicio', 'dia_semana', 'tipo_entrada', 'incidente_c4_rec'])
        df_predicciones = pd.concat([x_test_sin_dummies.assign(ano=2020), df_aux], axis=1)
        df_predicciones.drop(['y_test'], axis=1, inplace=True)


        #guardamos las prediciones para X_test
        with self.output()['predict_info_mensual'].open('w') as outfile1:
            df_predicciones.to_csv(outfile1, sep='\t', encoding='utf-8', index=None, header=False)

        #guardamos el metadata del modelo
        with self.output()['meta_info_mensual'].open('w') as outfile2:
            metadata.to_csv(outfile2, sep='\t', encoding='utf-8', index=None, header=False)


    def output(self):
        output_path = "s3://{}/{}/{}/".\
                      format(self.bucket,
                             self.root_path,
                             self.folder_path
                            )
        return {'predict_info_mensual' : luigi.contrib.s3.S3Target(path=output_path+'predicciones_mes_'+self.month+'_ano_'+self.year+'.csv'),
                'meta_info_mensual' : luigi.contrib.s3.S3Target(path=output_path+'metadata_predicciones_mes_'+self.month+'_ano_'+self.year+'.csv')}







class CreaTablaMetadatosPrediccionesInfoMensual(PostgresQuery):
    "Crea la tabla de metadatos para las predicciones de la Info Mensual"
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
            CREATE TABLE prediccion.Metadatos(fecha_de_ejecucion VARCHAR,
                                              ip_address VARCHAR,
                                              usuario VARCHAR,
                                              archivo_modelo VARCHAR,
                                              archivo_metadatos_modelo VARCHAR,
                                              mes_de_prediccion VARCHAR,
                                              ano_de_prediccion VARCHAR
                                              ); 
            """

    def requires(self):
         return CreaEsquemaPrediccion(self.db_instance_id, self.subnet_group, self.security_group,
                                      self.host, self.database, self.user, self.password)




class InsertaMetadatosPrediccionesInfoMensual(CopyToTable):
    """
    Esta funcion inserta los metadatos de Feature Engineering
    """
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
    #Parametros del bucket
    bucket = luigi.Parameter()
    root_path = luigi.Parameter()

    table = "prediccion.Metadatos"

    columns=[("fecha_de_ejecucion", "VARCHAR"),
             ("ip_address", "VARCHAR"),
             ("usuario", "VARCHAR"),
             ("archivo_modelo", "VARCHAR"),
             ("archivo_metadatos_modelo", "VARCHAR"),
             ("mes_de_prediccion", "VARCHAR"),
             ("ano_de_prediccion", "VARCHAR")]


    def rows(self):
        #Leemos el df de metadatos
        with self.input()['infile1']['meta_info_mensual'].open('r') as infile:
             for line in infile:
                  yield line.strip("\n").split("\t")


    def requires(self):
        return {"infile1" : PrediccionesInfoMensual(self.month, self.year,
                                                    self.db_instance_id, self.subnet_group, self.security_group,
                                                    self.database, self.user, self.password, self.host,
                                                    self.bucket, self.root_path),
                "infile2" : CreaTablaMetadatosPrediccionesInfoMensual(self.db_instance_id, self.subnet_group, self.security_group,
                                                                      self.database, self.user, self.password, self.host)}





class Test1ForPrediccionesInfoMensual(luigi.Task):
    "Corre las pruebas unitarias para la parte de Predicciones"

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

    folder_path = '0.pruebas_unitarias'
    test_error = 'none'
    def requires(self):
        return InsertaMetadatosPrediccionesInfoMensual(self.month, self.year,
                                                       self.db_instance_id, self.subnet_group, self.security_group,
                                                       self.database, self.user, self.password, self.host,
                                                       self.bucket, self.root_path)

    def run(self):
        prueba_predicciones_marbles = TestsForPredicciones()
        try:
            prueba_predicciones_marbles.test_check_porcentaje_1s()
            metadatos = funciones_req.metadata_para_pruebas_unitarias('test_check_porcentaje_1s','success','predicciones', self.test_error)
            with self.output().open('w') as outfile:
                metadatos.to_csv(outfile, sep='\t', encoding='utf-8', index=None, header=False)
        except ValueError as error:
            self.test_error = error
            raise ValueError("*****  ( ╯°□°)╯ ┻━━┻ ***** \n")

    def output(self):
        output_path = "s3://{}/{}/{}/".\
                    format(self.bucket,
                           self.root_path,
                           self.folder_path
                           )
        return luigi.contrib.s3.S3Target(path=output_path+"metadatos_prueba1_PREDICT_info_mensual_mes_"+self.month+"_ano_"+self.year+".csv")





@Test1ForPrediccionesInfoMensual.event_handler(luigi.Event.FAILURE)
def FailureTest1Predicciones(self, exception):
    """Will be called directly after a failed execution
       of `run` on any JobTask subclass
    """
    metadatos = funciones_req.metadata_para_pruebas_unitarias('test_check_porcentaje_1s','failed','predicciones', str(self.test_error))
    funciones_rds.insert_metadatos_unit_test(metadatos.to_records(index=False), self.database, self.user, self.password, self.host)




class InsertaMetadatosPruebasUnitariasPrediccionesInfoMensual(CopyToTable):
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
        return  { "infile1": Test1ForPrediccionesInfoMensual(self.month, self.year,
                                                      self.db_instance_id, self.subnet_group, self.security_group,
                                                      self.database, self.user, self.password, self.host,
                                                      self.bucket, self.root_path),
                  "infile2": Test2ForPrediccionesInfoMensual(self.month, self.year,
                                                      self.db_instance_id, self.subnet_group, self.security_group,
                                                      self.database, self.user, self.password, self.host,
                                                      self.bucket, self.root_path)}
