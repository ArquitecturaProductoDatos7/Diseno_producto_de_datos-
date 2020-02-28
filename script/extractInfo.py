import json
import os
from datetime import date

import luigi
import numpy as np

import datosCDMX


TARGET_PATH = os.path.join(os.path.dirname(__file__), 'data/{date}'.format(date=date.today()))


class FetchUserList(luigi.Task):
    """
    Fetches information to a JSON file.
    """
    
    def run(self):
        print('hola mndo')
        with self.output().open('w') as output_file:
            output_file.write("test")

    def output(self):
        return luigi.LocalTarget("incidentes_viales.json")
        
    