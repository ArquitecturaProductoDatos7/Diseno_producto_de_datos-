
import boto3
import psycopg2

def create_db_instance():

    """
    Función que crea la instancia de bases de datos en AWS con el manejador de datos postgres.
    """
    rds = boto3.client('rds')

    try:
        response = rds.create_db_instance(
            DBName = 'db_accidentes_cdmx',
            DBInstanceIdentifier='db-dpa',
            MasterUsername='postgres',
            MasterUserPassword='passwordDB',
            DBInstanceClass='db.t2.micro',
            Engine='postgres',  
            #EngineVersion='11.5-R1',
            #StorageType='SSD',
            AllocatedStorage=123,
            #MaxAllocatedStorage=1000,
            #VpcSecurityGroupIds=['vpc-0f463d794659a7eb1'],
            #DBSubnetGroupName='default-vpc-0f463d794659a7eb1' #descomentar esto cuando se tenga la vpc que requiere rds
            )
        #print (response)
        print("Successfully create DB instance")
    
    except Exception as error:
        print ("Couldn't create DB instance",error)

def connect():
    
    """
    Función que realiza la conexión a la base de datos que se especificó en la función anterior.
    """
    try:
        connection=psycopg2.connect(
            host=db_instance_endpoint, #poner el endpoint que haya resultado al crear la instancia de la funcion anterior
            port=5432,
            user='postgres',
            password='passwordDB',
            database='db_accidentes_cdmx')
        return(connection)

    except Exception as error:
        print ("I am unable to connect to the database", error)

def create_schemas():

    """
    Función que crea esquemas en la base de datos.
    """
    connection=connect()
    cursor=connection.cursor()
    sql='DROP SCHEMA IF EXISTS raw cascade; CREATE SCHEMA raw;'
    try:
        cursor.execute(sql)
        connection.commit()
    
    except Exception as error:
        print ("I can't create schema raw", error)
    
    cursor.close()
    connection.close()

def create_raw_tables():

    """
    Función que crea tablas (datos y metadatos) en el esquema raw.
    """
    connection=connect()
    cursor=connection.cursor()
    sql1=("""
        CREATE TABLE raw.IncidentesViales (
            id SERIAL PRIMARY KEY NOT NULL,
            incidentes_viales json NOT NULL
            );
        """)
    sql2=("""
        CREATE TABLE raw.metadatos(
            dataset TEXT,
            timezone TEXT,
            rows TEXT,
            format TEXT,
            refine_ano TEXT,
            refine_mes TEXT,
            fecha_ejecucion TEXT,
            parametros_url TEXT,
            ip_address TEXT,
            usuario TEXT,
            nombre_archivo TEXT,
            ruta TEXT
            );
        """)
    try:
        cursor.execute(sql1)
        connection.commit()
        cursor.execute(sql2)
        connection.commit()
    
    except Exception as error:
        print ("I can't create tables", error)
    
    cursor.close()
    connection.close()


    
#los nombres de las variables se podrían meter en un archivo csv y hacer referencia a ellas para que no se vea explicito
#(como lo hizo lilina)
