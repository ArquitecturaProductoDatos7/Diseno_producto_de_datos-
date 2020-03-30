import json, os, datetime, boto3, luigi, requests
import luigi.contrib.s3
import requests
import pandas as pd
import getpass
import socket   #para ip de metadatos
import funciones_rds
import funciones_s3


class ImprimeInicio(luigi.Task):
    """ Esta tarea de hace un print para indicar el inicio del ETL"""
    
    task_complete =False

    def run(self):
        print("*****Inicia tarea*****")
        self.task_complete=True

    # Como no genera un output, especifico un complete para que luigi sepa que ya acabó
    def complete(self):
        return self.task_complete


class Peticion_api_info_mensual(luigi.Task):
    """
    Esta tarea obtiene los registros de la API por mes y ano, desde el ano y mes especificado hasta la fecha de hoy - 2 meses
        mes es un entero para el mes que se quiere: 1,2,3,...,12
        ano es un entero desde 2014 hasta la fecha
    """
    
    data_url = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5"

    # Estos parámetros se tienen que especificar cuando se llama la tarea 
    url = luigi.Parameter(default=data_url)
    year = luigi.IntParameter()  # El ano de inicio para la extraccion de los datos
    month = luigi.IntParameter() # El mes de inicio para la extraccion de los datos
    
    
    # Estos parametros son internos de la tarea
    bucket = "bucket-dpa-2020"
    root_path = "incidentes_viales_CDMX"
    etl_path = "raw"
    file = ''
    ext = ''

    
    def requires(self):
        # Indica que se debe hacer primero la tarea anterior (ImprimeInicio)
        return ImprimeInicio()

    def run(self):

        date_start = datetime.date(self.year, self.month, 1)
        date_today = datetime.date.today()
        date_end = datetime.date(date_today.year, date_today.month - 2, 31)
        hostname = socket.gethostname()     
        ip_address = socket.gethostbyname(hostname)
        date_time = datetime.datetime.now()
        # Calcula los meses desde la fecha de inicio hasta 
        dates = pd.period_range(start=str(date_start), end=str(date_end), freq='M')

        for date in dates:
            self.year = date.year
            self.month = date.month
            self.file = 'incidentes_viales_'
            self.ext = 'json'

            #rows = -1 indica todos los registros
            parameters = {'rows': -1, 'refine.mes':self.month, 'refine.ano':self.year}
            #print(parameters)
            raw = requests.get(self.url, params = parameters)
            print("******* Estatus ******\n", raw.status_code)
            print("Ano: ", self.year, "Mes: ", self.month)

            #En cada ciclo se obtienen algunos parametros para el metadata
            metadata2 = {'fecha_ejecucion': date_time.strftime("%d/%m/%Y %H:%M:%S"),
                         'parametros_url': self.url,
                         #'parametros': parameters,
                         'ip_address': ip_address,
                         'usuario': getpass.getuser(),
                         'nombre_archivo': 'incidentes_viales_{}{}.json'.format(self.month,self.year),
                         'ruta': 's3://{}/{}/{}/YEAR={}/MONTH={}/'.format(self.bucket, self.root_path, 
                                                                          self.etl_path, self.year, 
                                                                          self.month),
                         #'tipo_datos': 'json'
                         }

            # Se especifica que es tipo json y se separan los records de los parametros
            out = raw.json()
            records = out['records']
            metadata = out['parameters']

            # Se juntan toda la metadata (parametros de la tarea + metadata2)
            # Se normaliza a csv
            metadata['metadata'] = metadata2
            metadata = pd.io.json.json_normalize(metadata).to_string(index=False)
            
            def to_upsert():
                """ Esta funcion extrae los metadatos que se desean guardar """
                
                return (out['parameters']['dataset'], out['parameters']['timezone'], 
                        out['parameters']['rows'], out['parameters']['format'], 
                        out['parameters']['refine']['ano'], out['parameters']['refine']['mes'], 
                        out['parameters']['metadata']['fecha_ejecucion'], 
                        out['parameters']['metadata']['parametros_url'], 
                        out['parameters']['metadata']['ip_address'], 
                        out['parameters']['metadata']['usuario'], 
                        out['parameters']['metadata']['nombre_archivo'], 
                        out['parameters']['metadata']['ruta'])
            
            # Conexion a la instancia RDS
            connection = funciones_rds.connect()
            cursor = connection.cursor()
            # Insertamos la informacion en la tabla raw.metadatos
            sql = ("""
                      INSERT INTO raw.metadatos(dataset, timezone, rows, format, refine_ano, refine_mes, fecha_ejecucion, parametros_url, ip_address, usuario, nombre_archivo, ruta) VALUES
                   (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                  """)
            record_to_insert = to_upsert()
            
            cursor.execute(sql, record_to_insert)
            connection.commit()
            # Cerramos la conexion
            cursor.close()
            connection.close()
            

            # Guardamos la info en un S3
            ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
            s3_resource = ses.resource('s3')
            obj = s3_resource.Bucket(self.bucket)

            # Este es el archivo json
            with self.output().open('w') as output:
                #output.write(self.raw.json())
                json.dump(records,output)

            # Se cambian parametros para guardar los metadatos y se guarda el csv
            self.file = 'metadatos'
            self.ext = 'csv'

            with self.output().open('w') as output:
                output.write(metadata)


    def output(self):
        # Tambien se guarda la informacion en un bucket
        output_path = "s3://{}/{}/{}/YEAR={}/MONTH={}/{}{}{}.{}".\
        format(self.bucket, 
               self.root_path,
               self.etl_path,
               self.year,
               self.month,
               self.file,
               self.month,
               self.year,
               self.ext
              )
        
        return luigi.contrib.s3.S3Target(path=output_path)
