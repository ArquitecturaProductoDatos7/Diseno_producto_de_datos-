import unittest
import pandas as pd
import numpy as np
import psycopg2
import datetime
import marbles.core
import pandas.io.sql as psql
from marbles.mixins import mixins
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

        #Para que falle la prueba
        #date_end = datetime.date.today()

        dates = pd.period_range(start=str(date_start), end=str(date_end), freq='M')
        n_periodo = len(dates)


        self.assertEqual(n_descargados[0], n_periodo, note="El número de meses del período (2014 - fecha) no coincide con el número de meses descargado")


    def test_check_num_archivos_info_mensual(self):
        #Numero de meses descargados
        cursor = self.connection.cursor()
        cursor.execute("SELECT count(*) FROM raw.metadatos WHERE refine_ano=\'\"2020\"\' and refine_mes=\'\"4\"\';")
        n_descargados = cursor.fetchone()
        self.connection.commit()

        #Para que falle la prueba
        #n_periodo = 2
        n_periodo = 1
        self.assertEqual(n_descargados[0], n_periodo, note="El número de meses requerido no coincide con el número de meses descargados")




    def test_check_num_registros(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT sum(rows) FROM raw.metadatos")
        n_rows = cursor.fetchall()
        self.connection.commit()

        cursor.execute("SELECT count(*) FROM raw.IncidentesVialesJSON")
        n_reg = cursor.fetchall()
        self.connection.commit()

        self.assertEqual(n_rows, n_reg, note="El número de registros extraídos y el número de registros cargados no son iguales")


    def test_check_num_registros_info_mensual(self):
       cursor = self.connection.cursor()
       cursor.execute("SELECT rows FROM raw.metadatos WHERE refine_ano=\'\"2020\"\' and refine_mes=\'\"4\"\';")
       n_rows = cursor.fetchone()
       self.connection.commit()

       cursor.execute("select count(*) from raw.infomensual;")
       n_reg = cursor.fetchone()
       self.connection.commit()

       self.assertEqual(n_rows[0], n_reg[0], note="El número de registros extraídos y el número de registros cargados no son iguales")







class TestClean(marbles.core.TestCase):
    """
    Clase para ejecutar las pruebas unitarias en la etapa clean
    1.- Probar que la columna delegacion_inicio de la base de datos *cleaned*
        este en minísculas
    2.- probar que la columna *mes* de la base de datos *cleaned*
        tenga el tipo correcto, en este caso "int.64"
    """
    def __init__(self, fname):
        #Buscamos el archivo del bucket en la S3
        self.data = funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/1.preprocesamiento/base_procesada.csv')

        host = funciones_rds.db_endpoint('db-dpa20')
        connection = funciones_rds.connect( 'db_incidentes_cdmx', 'postgres', 'passwordDB', host)
        #cleaned.IncidentesVialesInfoMensual
        self.dataframe = psql.read_sql("SELECT * FROM {};".format(fname), connection)

    def test_islower_w_marbles(self):
        column1 = self.data['delegacion_inicio']

        self.assertTrue(column1.str.islower().all())

    def test_islower_w_marbles_info_mensual(self):
        column1 = self.dataframe['delegacion_inicio']

        self.assertTrue(column1.str.islower().all())

    def test_correct_type(self):
        mes=self.data['mes']

        self.assertTrue(mes.dtype == np.int64, msg="El tipo de variable no es el correcto")

        #Con este ejemplo no pasaría la prueba
        #self.assertTrue(mes.dtype == np.object, msg="El tipo de variable no es el correcto")

    def test_correct_type_info_mensual(self):
        mes=self.dataframe['mes']
        print(mes.dtype)

        self.assertTrue(mes.dtype == np.int64, msg="El tipo de variable no es el correcto")





class TestFeatureEngineeringMarbles(marbles.core.TestCase):
    """
    Clase con pruebas de feature engineering usando marbles:
    1.- Probar que el número de categorías en la variable incidente_c4_rec sea 5, ya que así se reclasificó
    2.- Probar que se cumpla la condición de que el número de nulos en el set de entrenamiento para todas las variables es cero
    """

    def __init__(self, file1, file2, file3):
        super(TestFeatureEngineeringMarbles, self).__init__()
        #3.imputaciones/X_info_mensual_mes_4_ano_2020.csv
        self.data_procesada_info_mensual= funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/{}'.format(file1))
        #1.preprocesamiento/base_procesada.csv
        self.data_procesada = funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/{}'.format(file2))
        #3.imputaciones/X_train
        self.data_entrenamiento = funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/{}'.format(file3))

    def test_uniques_incidente_c4_rec(self):
        unicos = self.data_procesada['incidente_c4_rec'].nunique()

        self.assertEqual(unicos, 5, note="El número de categorías de la columna incidente_c4_rec es diferente de 5")

        #con este ejemplo no pasaría la prueba
        #self.assertEqual(unicos, 4, note="El número de categorías de la columna incidente_c4_rec es diferente de 5")

    def test_uniques_incidente_c4_rec_info_mensual(self):
        unicos2 = self.data_procesada_info_mensual['incidente_c4_rec'].nunique()

        self.assertEqual(unicos2, 5, note="El número de categorías de la columna incidente_c4_rec es diferente de 5")


    def test_nulls_x_train(self):
        condicion_nulos = self.data_entrenamiento.isnull().sum().all()

        self.assertTrue(condicion_nulos==0)

    def test_nulls_x_train_info_mensual(self):
        condicion_nulos = self.data_procesada_info_mensual.isnull().sum().all()

        self.assertTrue(condicion_nulos==0)



class TestFeatureEngineeringPandas(unittest.TestCase):
    """
    Clase con pruebas de feature engineering usando marbles y pandas:
    1.- Probar que el número de columnas en el set de entrenamiento después de hacer el one hote encoder sea igual al número de categorías de cada variable categórica, más las variables numéricas.
    2.- Probar que todas las variables sean numericas en el set de entrenamiento.
    """
    def __init__(self, file1, file2, file3, file4):
        #3.imputaciones/X_info_mensual_mes_4_ano_2020.csv
        self.data_procesada_antes_OneHoteEncoder_info_mensual= funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/{}'.format(file1))
        #4.input_modelo/X_info_mensual_mes_4_ano_2020.csv
        self.data_procesada_despues_OneHoteEncoder_info_mensual= funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/{}'.format(file2))

        #3.imputaciones/X_train.csv
        self.data_entrenamiento_antes_OneHoteEncoder = funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/{}'.format(file3))
        #4.input_modelo/X_train_input.csv
        self.data_entrenamiento_despues_OneHoteEncoder= funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/{}'.format(file4))

    def test_num_columns_x_train(self):
        lista_variables_categoricas=self.data_entrenamiento_antes_OneHoteEncoder.select_dtypes(include = 'object').columns.values
        archivo_variables_categoricas = self.data_entrenamiento_antes_OneHoteEncoder.loc[:, lista_variables_categoricas]
        categorias_total=archivo_variables_categoricas.nunique().sum()

        lista_variables_numericas=self.data_entrenamiento_antes_OneHoteEncoder.select_dtypes(include = 'number').columns.values
        total_columnas=len(lista_variables_numericas)+categorias_total
        total_columnas_despues_OneHoteEncoder=len(self.data_entrenamiento_despues_OneHoteEncoder.columns.values)

        self.assertEqual(total_columnas, total_columnas_despues_OneHoteEncoder)

    def test_num_columns_x_train_info_mensual(self):
        lista_variables_categoricas=self.data_procesada_antes_OneHoteEncoder_info_mensual.select_dtypes(include = 'object').columns.values
        archivo_variables_categoricas = self.data_procesada_antes_OneHoteEncoder_info_mensual.loc[:, lista_variables_categoricas]
        categorias_total=archivo_variables_categoricas.nunique().sum()

        lista_variables_numericas=self.data_procesada_antes_OneHoteEncoder_info_mensual.select_dtypes(include = 'number').columns.values
        total_columnas=len(lista_variables_numericas)+categorias_total
        total_columnas_despues_OneHoteEncoder=len(self.data_entrenamiento_despues_OneHoteEncoder.columns.values)

        self.assertEqual(total_columnas, total_columnas_despues_OneHoteEncoder)

    def test_numerical_columns_x_train(self):
        pd.api.types.is_numeric_dtype(self.data_entrenamiento_despues_OneHoteEncoder.values)

    def test_numerical_columns_x_train_info_mensual(self):
        pd.api.types.is_numeric_dtype(self.data_procesada_despues_OneHoteEncoder_info_mensual.values)





class TestsForPredicciones(marbles.core.TestCase, mixins.BetweenMixins):
    """
    Clase con pruebas para Predicciones usando marbles:
    1.- Probar que la proporcion de etiquetas verdaderas sea "parecida" a las de los datos de entrenamiento
    2.- Probar que el numero de columnas del df final es igual al numero de cols del df inicial +3
    """
    host = funciones_rds.db_endpoint('db-dpa20')
    connection = funciones_rds.connect( 'db_incidentes_cdmx', 'postgres', 'passwordDB', host)

    def __init__(self, fname):
        super(TestsForPredicciones, self).__init__()
        self.data = funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/6.predicciones_prueba/predicciones_modelo.csv')
        #Con esta FALLA la prueba
        #self.data = funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/2.separacion_base/y_test.csv')

        self.df_inicial= funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/2.separacion_base/X_test.csv')
        self.df_final = funciones_s3.abre_file_como_df('dpa20-incidentes-cdmx', 'bucket_incidentes_cdmx/{}'.format(fname))


    def test_check_porcentaje_1s(self):
        #Porcentaje de etiquetas verdaderas en las predicciones
        cursor = self.connection.cursor()
        cursor.execute("SELECT avg(y_etiqueta)*100 from prediccion.predicciones ; ")
        porcentaje_1_predicciones = cursor.fetchone()
        porcentaje_1_predicciones = float(porcentaje_1_predicciones[0])
        self.connection.commit()

        #Porcentaje de verdaderas en los datos
        porcentaje_1_historicos = self.data.iloc[:,9].mean()*100

        #Con esta FALLA la prueba
        #porcentaje_1_historicos = self.data['target'].mean()*100

        try:
            self.assertBetween(porcentaje_1_predicciones, lower=porcentaje_1_historicos*(0.8), upper=porcentaje_1_historicos*(1.2),
                               msg='El porcentaje de etiquetas verdaderas en las predicciones es muy diferente a la data historica')

        except marbles.core.marbles.ContextualAssertionError as error:
            raise ValueError(error)


    def test_check_num_cols_info_mensual(self):
        #cols en el df inicial
        cols_ini = self.df_inicial.columns
        #cols en el df inicial
        cols_fin = self.df_final.columns


        self.assertEqual(len(cols_ini)+4, len(cols_fin), note="El numero de variables en el df de predicciones no coincide con las vars consideradas al inicio")
