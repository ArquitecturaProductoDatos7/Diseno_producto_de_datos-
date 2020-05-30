# config: utf8
import boto3
import psycopg2
import pandas.io.sql as psql
from sqlalchemy import create_engine
import pandas as pd
#import numpy as np
#from sklearn.model_selection import train_test_split
#import pandas as pd
#from datetime import datetime
#from sklearn.impute import SimpleImputer




def create_db_instance(db_instance_id, db_name, db_user, db_pass, subnet_gp, security_gp):
    """
    Funcion que crea la instancia de bases de datos en AWS con el manejador de datos postgres.
    Input:
        db_name: Nombre de la base de datos
        db_user: Nombre del master user en la base de datos
        db_pass: Password para conectarse a la base de datos
        subnet_gp: Subnet Group para la base de datos
    """

    rds = boto3.client('rds', region_name='us-east-1')

    exito = 0
    try:

        response = rds.create_db_instance(
            DBName = db_name,
            DBInstanceIdentifier = db_instance_id,
            MasterUsername = db_user ,
            MasterUserPassword = db_pass,
            DBInstanceClass = 'db.t2.micro',
            Engine='postgres',  
            #EngineVersion='11.5-R1',
            #StorageType='SSD',
            AllocatedStorage=100,
            MaxAllocatedStorage = 1000,
            DBSubnetGroupName = subnet_gp, #descomentar esto cuando se tenga la vpc que requiere rds
            VpcSecurityGroupIds = [security_gp],
            )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print("***** Successfully create RDS instance *****\n ***** Waiting for RDS instance to be available *****")   
#            waiter = rds.get_waiter('db_instance_available')
#            waiter.wait(DBInstanceIdentifier = db_instance_id)
#            waiter.wait(DBInstanceIdentifier = db_instance_id, WaiterConfig = {"Delay": 100, "MaxAttempts": 7})
            exito = 1

    except rds.exceptions.DBInstanceAlreadyExistsFault:
        exito = 2
        print ("***** RDS instance already exists *****\n")

    except Exception as error:
        print ("***** Couldn't create RDS instance *****\n", error)

    return exito




def db_endpoint(db_instance_id):
    """ Esta funcion devuelve el Endpoint de la base db_name"""

    rds = boto3.client('rds', region_name='us-east-1')

#    print("***** RDS instance {} ready *****\n".format(db_instance_id))

    dbs = rds.describe_db_instances()

    endpoint = ""
    for i in range(0, len(dbs.get('DBInstances'))):
        if dbs.get('DBInstances')[i].get('DBInstanceIdentifier') == db_instance_id:
            endpoint = dbs.get('DBInstances')[i].get('Endpoint').get('Address')
            print('***** RDS instance Endpoint ready *****\n', endpoint)

    return endpoint




def connect(db_name, db_user, db_pass, db_endpoint):
    """
    Funcion que realiza la conexion a la base de datos que se especifico en la funcion anterior.
    Input:
        db_name: Nombre de la base de datos
        db_user: Nombre del master user en la base de datos
        db_pass: Password para conectarse a la base de datos
        db_endpoint: Endpoint para conectarse a la base
    """
    try:
        connection = psycopg2.connect(
                                    host = db_endpoint, #poner el endpoint que haya resultado al crear la instancia de la funcion anterior
                                    port = 5432,
                                    user = db_user,
                                    database = db_name,
                                    password = db_pass,
                                    )
#        print("***** This is the conexion *****\n", connection)
        return connection

    except Exception as error:
        print ("***** Unable to connect to the database *****\n", error)





def create_schemas(db_name, db_user, db_pass, db_endpoint):
    """
    Funcion que crea esquemas en la base de datos.
    """
    connection = connect(db_name, db_user, db_pass, db_endpoint)
    cursor = connection.cursor()
    sql = 'DROP SCHEMA IF EXISTS raw cascade; CREATE SCHEMA raw;'
    exito = 0
    try:
        cursor.execute(sql)
        connection.commit()
        exito = 1
        print('***** Schema raw created *****\n')
    except Exception as error:
        print ("***** Unable to create schema raw *****\n", error)

    cursor.close()
    connection.close()
    return exito


def create_raw_tables(db_name, db_user, db_pass, db_endpoint):
    """
    Funcion que crea tablas (datos y metadatos) en el esquema raw.
    """
    connection = connect(db_name, db_user, db_pass, db_endpoint)
    cursor = connection.cursor()
    sql1 = ("""
            CREATE TABLE raw.IncidentesViales(latitud FLOAT,
                                              folio VARCHAR,
                                              geopoint TEXT [],
                                              hora_creacion VARCHAR,
                                              delegacion_inicio VARCHAR,
                                              dia_semana VARCHAR,
                                              fecha_creacion VARCHAR,
                                              ano VARCHAR,
                                              tipo_entrada VARCHAR,
                                              codigo_cierre VARCHAR,
                                              hora_cierre VARCHAR,
                                              incidente_c4 VARCHAR,
                                              mes VARCHAR,
                                              delegacion_cierre VARCHAR,
                                              fecha_cierre VARCHAR,
			                      mesdecierre VARCHAR,
               	                              longitud FLOAT,
                                              clas_con_f_alarma VARCHAR
                                             );
          """)

    sql2 = ("""
            CREATE TABLE raw.metadatos(dataset TEXT,
                                       timezone TEXT,
                                       rows TEXT,
                                       refine_ano TEXT,
                                       refine_mes TEXT,
                                       parametro_url TEXT,
                                       fecha_ejecucion TEXT,
                                       ip_address TEXT,
                                       usuario TEXT,
                                       nombre_archivo TEXT,
                                       formato_archivo TEXT
                                      );
          """)

    sql3= ("""
            CREATE TABLE raw.IncidentesVialesJson(
                                                  properties JSON NOT NULL
                                                 );
            """)
#data_point_id SERIAL PRIMARY KEY NOT NULL,
    exito = 0
    try:
        cursor.execute(sql1)
        connection.commit()
        cursor.execute(sql2)
        connection.commit()
        cursor.execute(sql3)
        connection.commit()
        exito = 1
        print('***** Tables created*****\n') 
    except Exception as error:
        print ("***** Unable to create tables *****\n", error)

    cursor.close()
    connection.close()
    return exito





def bulk_insert_raw(records, meta, db_name, db_user, db_pass, db_endpoint):
    """
    Inserta los registros y el metadata del esquema RAW
    """
    try:
        connection = connect(db_name, db_user, db_pass, db_endpoint)
        cursor = connection.cursor()
        sql_insert_records = """ INSERT INTO raw.IncidentesVialesJson (registros) 
                                 VALUES (%s); """

        sql_insert_metadata = """ INSERT INTO raw.metadatos  (dataset, timezone,
                                                              rows, refine_ano, 
                                                              refine_mes, parametro_url,
                                                              fecha_ejecucion, ip_address, 
                                                              usuario, nombre_archivo, 
                                                              formato_archivo)

                                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s); """

        # executemany() to insert multiple rows rows
        cursor.executemany(sql_insert_records, records)
        connection.commit()
        print("***** {} Records inserted successfully into table *****".format(cursor.rowcount))
        cursor.executemany(sql_insert_metadata, meta)
        connection.commit()
        print("***** {} Metadata record inserted successfully *****".format(cursor.rowcount))

    except (Exception, psycopg2.Error) as error:
        print("***** Failed inserting record into table: {} *****".format(error))

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
#            print("***** PostgreSQL connection is closed *****")






def bulk_insert_cleaned(meta, db_name, db_user, db_pass, db_endpoint):
    "Inserta el metadata del esquema CLEANED"
    try:
        connection = connect(db_name, db_user, db_pass, db_endpoint)
        cursor = connection.cursor()

        sql_insert_metadata = """ INSERT INTO cleaned.Metadatos (fecha_ejecucion,
                                                                 ip_address,
                                                                 usuario,
                                                                 id_tarea,
                                                                 estatus_tarea,
                                                                 registros_eliminados)

                                  VALUES (%s, %s, %s, %s, %s, %s); """

        # executemany() to insert multiple rows rows
        cursor.executemany(sql_insert_metadata, meta)
        connection.commit()
        print("***** {} Metadata record for CLEANED inserted successfully *****".format(cursor.rowcount))

    except (Exception, psycopg2.Error) as error:
        print("***** Failed inserting record into table: {} *****".format(error))

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
#            print("***** PostgreSQL connection is closed *****")





def obtiene_df(db_name, db_user, db_pass, db_endpoint, db_table, db_schema):
    "Lee como df la tabla db_table del esquema db_schema en la base de datos"
    try:
        engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format( 
             user = db_user,
             password = db_pass,
             host = db_endpoint,
             port = "5432",
             database = db_name
             )
        #create sqlalchemy engine
        engine = create_engine(engine_string)

        #read a table from database
#        dataframe = pd.read_sql_table(table_name='incidentesviales', con=engine, schema='cleaned')
        dataframe = pd.read_sql_table(table_name=db_table, con=engine, schema=db_schema)

    except (Exception) as error:
        print("***** Failed getting df: {} *****".format(error))

    return dataframe





def insert_metadatos_unit_test(meta, db_name, db_user, db_pass, db_endpoint):
    "Inserta el metadata del esquema CLEANED"
    try:
        connection = connect(db_name, db_user, db_pass, db_endpoint)
        cursor = connection.cursor()

        sql_insert_metadata = """ INSERT INTO tests.pruebas_unitarias (fecha_ejecucion,
                                                                       ip_address,
                                                                       usuario,
                                                                       test,
                                                                       test_status,
                                                                       level,
                                                                       error)
                                  VALUES (%s, %s, %s, %s, %s, %s, %s); """

        # executemany() to insert multiple rows rows
        cursor.executemany(sql_insert_metadata, meta)
        connection.commit()
       # print("***** {} Metadata record for PRUEBAS UNITARIAS inserted successfully *****".format(cursor.rowcount))

    except (Exception, psycopg2.Error) as error:
        print("***** Failed inserting record into table: {} *****".format(error))

    finally:
        # closing database connection.
        if (connection):
           cursor.close()
           connection.close()
           #print("***** PostgreSQL connection is closed *****")
