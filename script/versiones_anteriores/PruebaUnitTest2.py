import unittest
import pandas as pd
import funciones_s3
from pandas.util.testing import assert_frame_equal # <-- for testing dataframes
import marbles.core

class TestFeatureEngineering1(marbles.core.TestCase):

    """ 
    Clase para probar que el número de categorías en la variable incidente_c4_rec sea 5, 
    ya que así se reclasificó
    
    """
    @classmethod
    def setUpClass(cls):
        
        """ Your setUp """
        try:
            data =funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/1.preprocesamiento/base_procesada.csv')
        except IOError:
            print ('cannot open file', IOError)
        cls.unicos=data['incidente_c4_rec'].nunique()
        cls.column1=data['delegacion_inicio']
    
    def test_colum_Incidente_c4_rec_uniques(self):
        self.assertEqual(self.unicos, 5, note="El número de categorías de la columna incidente_c4_rec es diferente de 5")
             
    def test_islower_w_marbles(self):
        self.assertTrue(self.column1.str.islower().all())
