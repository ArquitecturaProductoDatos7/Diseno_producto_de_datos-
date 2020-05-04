import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn_pandas import CategoricalImputer
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
import pickle


def preprocesamiento_variable(df):
    """
    Esta funcion realiza los siguientes cambios al df:
        1. Recategoriza la variable 'incidente_c4'
        2. Crea la variable 'hora' que guarda la solo la hora de la columna 'hora_creacion'
        3. Crea la variable objetivo 'target' 
    """

    # Split para las categorias
    df['incidente_c4_rec'] = df['incidente_c4'].str.split('-').str[0] 

    # Crear la variable hora que solo contenga la hora
    df['hora'] = df['hora_creacion'].astype('str').str.split(':').str[0]
    # Crear la variable target
    df['clave'] = df['codigo_cierre'].str[1:2]  # Obtiene la letra (categoria)
    df['target']=np.where(df['clave']=='a',1,0)  # la convierte a 0/1

    return df


def separa_train_y_test(df, vars_mod, var_obj):
    "Separacion de mi set de entrenamiento y prueba"

    PORCENTAJE_TEST = 0.3

    #Seleccionamos las variables del modelo
    datos_select=df[vars_mod]

    #Hacemos la separacion
    X_train, X_test, y_train, y_test = train_test_split(datos_select.loc[:, datos_select.columns != var_obj],
                                                         datos_select[[var_obj]], test_size=PORCENTAJE_TEST, random_state=0)


    return X_train, X_test, y_train, y_test



def imputacion_variable_delegacion(X_train, X_test):
    " Esta funcion imputa la variable 'delegacion_inicio' con la moda "

    #Para el set de entrenamiento
    X = X_train.delegacion_inicio.values.reshape(X_train.shape[0],1)
    delegacionInicio_imputer=CategoricalImputer(strategy='most_frequent')
    X_train['delegacion_inicio']=delegacionInicio_imputer.fit_transform(X)

    #Para el set de prueba
    X = X_test.delegacion_inicio.values.reshape(X_test.shape[0],1)
    X_test['delegacion_inicio']=delegacionInicio_imputer.transform(X) 


    return X_train, X_test




def dummies_para_categoricas(X_train, X_test):
    "Esta funcion convierte variables categoricas a dummies"

    lista_vars_categoricas = X_train.select_dtypes(include = 'object').columns.values

    train_vars_input = pd.get_dummies(X_train, columns = lista_vars_categoricas, prefix=lista_vars_categoricas)

    test_vars_input = pd.get_dummies(X_test, columns = lista_vars_categoricas, prefix=lista_vars_categoricas)

    return train_vars_input, test_vars_input




#Para Ranfom Forest
def magic_loop_ramdomF(X_train,y_train, hyper_params_grid):
    """
       Esta funcion ajusta el modelo de Random Forest con diferentes hiperparametros y regresa:
             - la informacion de todos los modelos, rankeados segun la metrica elegida
             - el nombre del pickle donde se guarda el *mejor* modelo ya entrenado
    """
    #Convertimos de column vector a 1d array
    y_train = y_train.values.ravel()

    classifier = RandomForestClassifier()

    grid_search = GridSearchCV(classifier,
                               hyper_params_grid,
                               scoring = 'precision',
                               #scoring = 'recall'
                               cv = 10, 
                               n_jobs = -1)
                               #verbose = 3)

    grid_search.fit(X_train, y_train)

    #pickle
#    filename = 'finalized_model.pkl'
#    pickle.dump(grid_search, open(filename, 'wb'))

    cv_results = pd.DataFrame(grid_search.cv_results_)
    results = cv_results.sort_values(by='rank_test_score', ascending=True)

    return results, grid_search

#Para Regresión logística
def magic_loop_RL(X_train,y_train, hyper_params_grid):
    """
       Esta funcion ajusta el modelo de Regresión Logística con diferentes hiperparametros y regresa:
             - la informacion de todos los modelos, rankeados segun la metrica elegida
             - el nombre del pickle donde se guarda el *mejor* modelo ya entrenado
    """
    #Convertimos de column vector a 1d array
    y_train = y_train.values.ravel()

    classifier = LogisticRegression(solver='liblinear')

    grid_search = GridSearchCV(classifier,
                               hyper_params_grid,
                               scoring = 'precision',
                               #scoring = 'recall'
                               cv = 2, 
                               n_jobs = -1)
                               #verbose = 3)

    grid_search.fit(X_train, y_train)

    cv_results = pd.DataFrame(grid_search.cv_results_)
    results = cv_results.sort_values(by='rank_test_score', ascending=True)

    return results, grid_search

#Para XGboost
def magic_loop_GB(X_train,y_train, hyper_params_grid):
    """
       Esta funcion ajusta el modelo de GradientBoosting con diferentes hiperparametros y regresa:
             - la informacion de todos los modelos, rankeados segun la metrica elegida
             - el nombre del pickle donde se guarda el *mejor* modelo ya entrenado
    """
    #Convertimos de column vector a 1d array
    y_train = y_train.values.ravel()

    classifier = GradientBoostingClassifier()

    grid_search = GridSearchCV(classifier,
                               hyper_params_grid,
                               scoring = 'precision',
                               #scoring = 'recall'
                               cv = 2, 
                               n_jobs = -1)
                               #verbose = 3)

    grid_search.fit(X_train, y_train)

    cv_results = pd.DataFrame(grid_search.cv_results_)
    results = cv_results.sort_values(by='rank_test_score', ascending=True)

    return results, grid_search

