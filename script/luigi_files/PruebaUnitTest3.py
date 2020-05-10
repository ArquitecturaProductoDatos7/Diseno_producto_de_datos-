#import unittest
import pandas as pd
import funciones_s3
#from pandas.util.testing import assert_frame_equal # <-- for testing dataframes
import marbles.core
import numpy as np

class TestFeatureEngineering1(marbles.core.TestCase):
    """ 
    Clase para ejecutar las pruebas unitaria
    """

    #Buscamos el archivo del bucket en la S3
    data = funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/1.preprocesamiento/base_procesada.csv')
 
    def test_colum_Incidente_c4_rec_uniques(self):
        """ 
        Función para probar que el número de categorías en la variable incidente_c4_rec sea 5, 
        ya que así se reclasificó.
        """
        unicos = self.data['incidente_c4_rec'].nunique()

        self.assertEqual(unicos, 5, note="El número de categorías de la columna incidente_c4_rec es diferente de 5")

    def test_islower_w_marbles(self):
        """ 
        Función para probar que la columna delegacion_inicio de la base de datos *cleaned* 
        este en minísculas
        """
        column1 = self.data['delegacion_inicio']

        self.assertTrue(column1.str.islower().all())
    
    def test_correct_type(self):
        """ 
        Función para probar que la columna mes de la base de datos *cleaned* 
        tenga el tipo correcto
        """
        mes=self.data['mes']
        
        self.assertTrue(mes.dtype == np.int64)
        #self.assertTrue(mes.dtype == np.object)  #Con este ejemplo no pasaría la prueba
        
        
