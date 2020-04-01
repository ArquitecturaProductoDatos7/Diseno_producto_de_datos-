import boto3
import psycopg2


def create_db_instance(db_instance_id, db_name, db_user, db_pass, subnet_gp, security_gp):
    """
    Funcion que crea la instancia de bases de datos en AWS con el manejador de datos postgres.
    Input:
        db_name: Nombre de la base de datos
        db_user: Nombre del master user en la base de datos
        db_pass: Password para conectarse a la base de datos
        subnet_gp: Subnet Group para la base de datos
    """
        
    rds = boto3.client('rds')
    
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
            exito = 1
            print("***** Successfully create RDS instance *****\n")    
        
    except rds.exceptions.DBInstanceAlreadyExistsFault:
        exito = 1
        print ("***** RDS instance already exists *****\n")
        
    except Exception as error:
        print ("***** Couldn't create RDS instance *****\n", error)
        
    
    return exito



        
def db_endpoint(db_instance_id):
    """ Esta funcion devuelve el Endpoint de la base db_name"""
    
    print("***** waiting for RDS instance to be ready...zzz... *****\n")
    #esperamos a que la instancia este disponible (180 sec e intentamos 5 veces)
    rds = boto3.client('rds')
    waiter = rds.get_waiter('db_instance_available')
    waiter.wait(DBInstanceIdentifier = db_instance_id, 
                WaiterConfig = {"Delay": 120, "MaxAttempts": 15},
               )
    print("***** RDS instance {} ready *****\n".format(db_instance_id))
    
    dbs = rds.describe_db_instances()
    
    endpoint = ""
    for i in range(0, len(dbs.get('DBInstances'))):
        if dbs.get('DBInstances')[i].get('DBInstanceIdentifier') == db_instance_id:
            endpoint = dbs.get('DBInstances')[i].get('Endpoint').get('Address')
            print('***** RDS instance Endpoint *****\n',endpoint)
    
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
    #client = boto3.client('s3')
    connection = None
    try:
        connection = psycopg2.connect(
                                    host = db_endpoint, #poner el endpoint que haya resultado al crear la instancia de la funcion anterior
                                    port = 5432,
                                    user = db_user ,
                                    password = db_pass,
                                    database = db_name
                                    )
        return(connection)
    
    except Exception as error:
        print ("***** Unable to connect to the database *****\n", error)

        
        
def create_schemas(db_name, db_user, db_pass, db_endpoint):
    """
    Funcion que crea esquemas en la base de datos.
    """
    connection = connect(db_name, db_user, db_pass, db_endpoint)
    cursor = connection.cursor()
    sql = 'DROP SCHEMA IF EXISTS raw cascade; CREATE SCHEMA raw;'
    try:
        cursor.execute(sql)
        connection.commit()
    
    except Exception as error:
        print ("***** Unable to create schema raw *****\n", error)
    
    cursor.close()
    connection.close()

    
    
def create_raw_tables(db_name, db_user, db_pass, db_endpoint):
    """
    Funcion que crea tablas (datos y metadatos) en el esquema raw.
    """
    connection = connect(db_name, db_user, db_pass, db_endpoint)
    cursor = connection.cursor()
    sql1 = ("""
            CREATE TABLE raw.IncidentesViales (id SERIAL PRIMARY KEY NOT NULL, incidentes_viales json NOT NULL);
          """)
    
    sql2 = ("""
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
        print ("***** Unable to create tables *****\n", error)
    
    cursor.close()
    connection.close()


    
#los nombres de las variables se podrian meter en un archivo csv y hacer referencia a ellas para que no se vea explicito
#(como lo hizo lilina)
