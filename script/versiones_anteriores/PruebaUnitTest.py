import unittest
import pandas as pd
import funciones_s3
from pandas.util.testing import assert_frame_equal # <-- for testing dataframes
import marbles.core
from pandas import Series

class TestFeatureEngineering1(marbles.core.TestCase):

    """ 
    Clase para probar que el número de categorías en la variable incidente_c4_rec sea 5, 
    ya que así se reclasificó
    
    """

    def setUp(self):
        
        """ Your setUp """
        try:
            data =funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/1.preprocesamiento/base_procesada.csv')
        except IOError:
            print ('cannot open file')
        self.unicos=data['incidente_c4_rec'].nunique()
        #print(self.unicos)

    def test_colum_Incidente_c4_rec_uniques(self):
        self.assertEqual(self.unicos, 5, note="El número de categorías de la columna incidente_c4_rec es diferente de 5")
        
        
class TestFeatureEngineering2(marbles.core.TestCase):
    """ 
    Clase para probar que las columnas tipo object sean minúsculas
    """
    def setUp(self):
        
        """ Your setUp """
        try:
            data =funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx',
                                                 'bucket_incidentes_cdmx/1.preprocesamiento/base_procesada.csv')
        except IOError:
            print ('cannot open file')
        
        self.column1=data['delegacion_inicio']
            
    def test_islower_w_marbles(self):
        self.assertTrue(self.column1.str.islower().all())
