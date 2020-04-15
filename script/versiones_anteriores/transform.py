import requests
import json
import csv
import pandas as pd
from pandas.io.json import json_normalize

def tansf():
	#res = requests.get('./data.json').json()['records']
	with open('data.json', 'r') as file:
	    data = file.read()

	df = json_normalize(json.loads(data)['records'])
	df.to_csv('data.csv')
	
	return None

tansf()

