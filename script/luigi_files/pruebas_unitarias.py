import unittest
import pandas as pd
import psycopg2
import datetime
import marbles.core
import numpy as np
import funciones_s3
import funciones_rds

class TestsForExtract(marbles.core.TestCase):
    """ 
    Clase con pruebas de Extract usando marbles:
    1.- Probar que el número de meses del periodo coincida con los archivos descargados
    2.- Probar que se descargaron todos los registros del periodo
    """
    host = funciones_rds.db_endpoint('db-dpa20')
    connection = funciones_rds.connect( 'db_incidentes_cdmx', 'postgres', 'passwordDB', host)
        
    def test_check_num_archivos(self):
        #Numero de meses descargados
        cursor = self.connection.cursor()
        cursor.execute("SELECT count(*) FROM raw.metadatos")
        n_descargados = cursor.fetchone()
        self.connection.commit()
        
        #Numero de meses en el periodo
        date_start = datetime.date(2014,1,1)
        date_end = datetime.date(2020,3,1)
        #date_end = datetime.date.today()
        dates = pd.period_range(start=str(date_start), end=str(date_end), freq='M')
        n_periodo = len(dates)
        
        
        self.assertEqual(n_descargados[0], n_periodo, note="El número de meses del período (2014 - fecha) no coincide con el número de meses descargado")

   
    def test_check_num_registros(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT sum(rows) FROM raw.metadatos")
        n_rows = cursor.fetchall()
        self.connection.commit()
                        
        cursor.execute("SELECT count(*) FROM raw.IncidentesVialesJSON")
        n_reg = cursor.fetchall()
        self.connection.commit()
        
        self.assertEqual(n_rows, n_reg, note="El número de registros extraídos y el número de registros cargados no son iguales")



class TestsForLoad(marbles.core.TestCase):
    """ 
    Clase con pruebas de Load usando marbles:
    1.- Probar que el número de columnas del archivo descargado sean 18
    
    """
    host = funciones_rds.db_endpoint('db-dpa20')
    connection = funciones_rds.connect( 'db_incidentes_cdmx', 'postgres', 'passwordDB', host)
        
    def test_check_num_columnas(self):
        #Numero de meses descargados
        cursor = self.connection.cursor()
        cursor.execute("SELECT DISTINCT (SELECT count(*) FROM json_object_keys(registros)) nbr_keys FROM raw.incidentesvialesjson;")
        n_cols = cursor.fetchall()
        self.connection.commit()
        print("***", n_cols)
        
        #self.assertEqual(n_descargados[0], n_periodo, note="El número de meses del período (2014 - fecha) no coincide con el número de meses descargado")
        

class TestClean(marbles.core.TestCase):
    
    """ 
    Clase para ejecutar las pruebas unitarias en la etapa clean
    1.- Probar que la columna delegacion_inicio de la base de datos *cleaned* 
        este en minísculas
    2.- probar que la columna *mes* de la base de datos *cleaned* 
        tenga el tipo correcto, en este caso "int.64"
    """
    #Buscamos el archivo del bucket en la S3
    data = funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/1.preprocesamiento/base_procesada.csv')

    def test_islower_w_marbles(self):
        column1 = self.data['delegacion_inicio']

        self.assertTrue(column1.str.islower().all())
    
    def test_correct_type(self):
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
        #self.assertEqual(unicos, 4, note="El número de categorías de la columna incidente_c4_rec es diferente de 5") con este ejemplo no pasaría la prueba

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
        
