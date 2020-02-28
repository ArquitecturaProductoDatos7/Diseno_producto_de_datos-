import requests

DATAURL = "https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=incidentes-viales-c5"

def fetch_json(url):
    res = requests.get(url)
    return res