import unittest
import pandas as pd
import funciones_s3
import marbles.core
import numpy as np

class TestClean(marbles.core.TestCase):
    """ 
    Clase para ejecutar las pruebas unitarias en la etapa clean
    """
    #Buscamos el archivo del bucket en la S3
    data = funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/1.preprocesamiento/base_procesada.csv')

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
        
class TestFeatureEngineeringMarbles(marbles.core.TestCase):

    """ 
    Clase con pruebas de feature engineering usando marbles:
    1.- Probar que el número de categorías en la variable incidente_c4_rec sea 5, ya que así se reclasificó
    2.- Probar que se cumpla la condición de que el número de nulos en el set de entrenamiento para todas las variables es cero
    """

    data_procesada = funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/1.preprocesamiento/base_procesada.csv')
    
    data_entrenamiento = funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/3.Imputaciones/X_train.csv')
    
    
    def test_uniques_incidente_c4_rec(self):
        unicos = self.data_procesada['incidente_c4_rec'].nunique()

        self.assertEqual(unicos, 5, note="El número de categorías de la columna incidente_c4_rec es diferente de 5")

    def test_nulls_x_train(self):
        condicion_nulos = self.data_entrenamiento.isnull().sum().all()

        self.assertTrue(condicion_nulos==0)
        

class TestFeatureEngineeringPandas(unittest.TestCase):
    
    """ 
    Clase con pruebas de feature engineering usando marbles y pandas:
    1.- Probar que el número de columnas en el set de entrenamiento después de hacer el one hote encoder sea igual al número de categorías de cada variable categórica, más las variables numéricas.
    2.- Probar que todas las variables sean numericas en el set de entrenamiento.
    """
     
    data_entrenamiento_antes_OneHoteEncoder = funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/3.Imputaciones/X_train.csv')
    
    
    data_entrenamiento_despues_OneHoteEncoder=funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/4.input_modelo/X_train_input.csv')
    

    def test_num_columns_x_train(self):
        lista_variables_categoricas=self.data_entrenamiento_antes_OneHoteEncoder.select_dtypes(include =
                                                                                               'object').columns.values
        archivo_variables_categoricas = self.data_entrenamiento_antes_OneHoteEncoder.loc[:, lista_variables_categoricas]
        categorias_total=archivo_variables_categoricas.nunique().sum()
        
        lista_variables_numericas=self.data_entrenamiento_antes_OneHoteEncoder.select_dtypes(include =
                                                                                             'number').columns.values
        total_columnas=len(lista_variables_numericas)+categorias_total
        
        total_columnas_despues_OneHoteEncoder=len(self.data_entrenamiento_despues_OneHoteEncoder.columns.values)
        
        self.assertEqual(total_columnas, total_columnas_despues_OneHoteEncoder)
        
    def test_numerical_columns_x_train(self):
        
        pd.api.types.is_numeric_dtype(self.data_entrenamiento_despues_OneHoteEncoder.values)
        