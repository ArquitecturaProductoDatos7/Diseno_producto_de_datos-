import requests
import json
import pandas as pd
from pandas.io.json import json_normalize
from datetime import datetime, timedelta

DATAURL = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5"
DATAURL2 = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5&rows=1000"
DATAURL3 = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5&facet=folio&facet=fecha_creacion&facet=hora_creacion&facet=dia_semana&facet=codigo_cierre&facet=fecha_cierre&facet=ano_cierre&facet=mes_cierre&facet=hora_cierre&facet=delegacion_inicio&facet=incidente_c4&facet=latitud&facet=longitud&facet=clas_con_f_alarma&facet=tipo_entrada&facet=delegacion_cierre&facet=geopoint&facet=mes"

def extrae(url):
    res = requests.get(url).json()['records']
    df = json_normalize(res)
    df.to_csv('data.csv')
    return None

def extrae_dia(d='04',m='09',a='2015'):

    res = requests.get('https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5&rows=10000&refine.fecha_creacion={0}%2F{1}%2F{2}'.format(d,m,a)).json()['records']
    #res = requests.get('https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c57&facet=hora_creacion&facet=dia_semana&facet=codigo_cierre&facet=mesdecierre&facet=delegacion_inicio&facet=incidente_c4&facet=clas_con_f_alarma&facet=tipo_entrada&facet=mes&facet=folio&facet=fecha_creacion&facet=fecha_cierre&facet=hora_cierre&refine.fecha_creacion={0}%2F{1}%2F{2}'.format(d,m,a)).json()['records']
    df = json_normalize(res)
    return df

def extrae_total(desde='04092015', hasta='06092015'):

	fecha_inicio = datetime.strptime(desde,'%d%m%Y')
	fecha_fin = datetime.strptime(hasta,'%d%m%Y')

	df = pd.DataFrame()

	for t in range(int ((fecha_fin - fecha_inicio).days)):
		fecha = fecha_inicio + timedelta(t)
		fechas = datetime.strftime(fecha,'%d%m%Y')
		dia = fechas[:2]
		mes = fechas[2:4]
		ano = fechas[4:]

		extrae = extrae_dia(d=dia, m=mes, a=ano)
		#print(extrae)
		df = df.append(extrae)
	print(df)
	df.to_csv('data.csv')

	return None



#extrae(DATAURL3)

#extrae_dia()

extrae_total()