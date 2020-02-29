import json
import os
from datetime import date

import luigi
import numpy as np

import extrae


TARGET_PATH = os.path.join(os.path.dirname(__file__), 'data/{date}'.format(date=date.today()))
DATAURL = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5&facet=folio&facet=fecha_creacion&facet=hora_creacion&facet=dia_semana&facet=codigo_cierre&facet=fecha_cierre&facet=ano_cierre&facet=mes_cierre&facet=hora_cierre&facet=delegacion_inicio&facet=incidente_c4&facet=latitud&facet=longitud&facet=clas_con_f_alarma&facet=tipo_entrada&facet=delegacion_cierre&facet=geopoint&facet=mes"


class FetchUserList(luigi.Task):
    """
    Fetches information to a JSON file.
    """
    
    def run(self):
        response = extrae.fetch_json(DATAURL)
        with self.output().open('w') as fout:
            fout.write(response)
            

    def output(self):
        return get_local_target("incidentes_viales.json")
    

def get_local_target(name):
    return luigi.LocalTarget(os.path.join(TARGET_PATH, name))
    