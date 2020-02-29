import requests
import json

DATAURL = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5&facet=folio&facet=fecha_creacion&facet=hora_creacion&facet=dia_semana&facet=codigo_cierre&facet=fecha_cierre&facet=ano_cierre&facet=mes_cierre&facet=hora_cierre&facet=delegacion_inicio&facet=incidente_c4&facet=latitud&facet=longitud&facet=clas_con_f_alarma&facet=tipo_entrada&facet=delegacion_cierre&facet=geopoint&facet=mes"

def fetch_json(url):
    res = requests.get(url).json()
    res_str = json.dumps(res, indent=2)
    return res_str