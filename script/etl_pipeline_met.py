import json, os, datetime, boto3, luigi, requests
import luigi.contrib.s3
import requests
import pandas as pd
import getpass
import socket   #para ip de metadatos


## Esta tarea de ImprimeInicio solo hace un print, solo quería probar tareas sequenciales

class ImprimeInicio(luigi.Task):
    task_complete =False

    def run(self):
        print("*****Inicia tarea*****")
        self.task_complete=True

## Como no genera un output, especifico un complete para que luigi sepa que ya acabó
    def complete(self):
        return self.task_complete


class peticion_api_info_mensual(luigi.Task):
    """
    Esta funcion obtiene los registros de la API por mes y ano
        mes es un entero para el mes que se quiere: 1,2,3,...,12
        ano es un entero desde 2014 hasta la fecha
    """

    # Algunos parámetros son de luigi, entonces hay que definirlos cuando se manda a llamar la tarea. 
    # Si quieren lo podemos cambiar

    url = luigi.Parameter(default="https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5")
    month = luigi.IntParameter()
    year = luigi.IntParameter()

    bucket = "bucket-dpa-2020"
    root_path = "incidentes_viales_CDMX"
    etl_path = "raw"
    file = ''
    ext = ''

    #Aquí digo que se debe hacer primero la tarea anterior.
    def requires(self):
        return ImprimeInicio()

    def run(self):

        # A partir de aquí es lo que me traje al luigi lo que Bren había puesto en otra funcion

        date_start = datetime.date(self.year,self.month,1)
        date_today = datetime.date.today()
        date_end = datetime.date(date_today.year, date_today.month - 2, 31)
        hostname = socket.gethostname()     
        ip_address = socket.gethostbyname(hostname)

        dates = pd.period_range(start=str(date_start), end=str(date_end), freq='M')

        for date in dates:
            self.year = date.year
            self.month = date.month
            self.file = 'incidentes_viales_'
            self.ext = 'json'

            #rows = -1 indica todos los registros
            parameters = {'rows': -1, 'refine.mes':self.month, 'refine.ano':self.year}
            print(parameters)
            raw = requests.get(self.url, params = parameters)
            print("******* Estatus ****** ", raw.status_code)
            print(self.year)
            print(self.month)

            #Además cada ciclo obtiene algunos parametros para el metadata

            metadata2 = {'fecha_ejecucion': str(date_today),
            'parametros_url': self.url,
            #'parametros': parameters,
            'ip_address': ip_address,           
            'usuario': getpass.getuser(),
            'nombre_archivo': 'incidentes_viales_{}{}.json'.format(self.month,self.year),
            'ruta': 's3://{}/{}/{}/YEAR={}/MONTH={}/'.format(self.bucket, self.root_path, self.etl_path, self.year, self.month),
            #'tipo_datos': 'json'
            }

            # Aqui le digo a python que es un jason y separo los records de los parametros
            out = raw.json()
            records = out['records']
            metadata = out['parameters']

            #A los parametros le agrigo los metadatos de arriba para tener todo junto y se normaliza a csv
            metadata['metadata'] = metadata2
            metadata = pd.io.json.json_normalize(metadata).to_string(index=False)


            #guardamos la info en un S3
            ses = boto3.session.Session(profile_name='default', region_name='us-east-1')
            s3_resource = ses.resource('s3')
            obj = s3_resource.Bucket(self.bucket)

            #este es el archivo json
            with self.output().open('w') as output:
                #output.write(self.raw.json())
                json.dump(records,output)

            # Se cambian parametros para cambiar el nombre y se guarda el csv
            self.file = 'metadatos'
            self.ext = 'csv'

            with self.output().open('w') as output:
                output.write(metadata)


    def output(self):
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


    

