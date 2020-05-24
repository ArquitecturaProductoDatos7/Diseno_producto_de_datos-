import numpy as np
import pandas as pd
import socket, getpass
import datetime
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn_pandas import CategoricalImputer
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score, recall_score, precision_score, f1_score, confusion_matrix, auc, roc_curve
import pickle



def preprocesamiento_variable(df):
    """
    Esta funcion realiza los siguientes cambios al df:
        1. Recategoriza la variable 'incidente_c4'
        2. Crea la variable 'hora' que guarda la solo la hora de la columna 'hora_creacion'
    """

    # Split para las categorias
    df['incidente_c4_rec'] = df['incidente_c4'].str.split('-').str[0] 

    # Crear la variable hora que solo contenga la hora
    df['hora'] = df['hora_creacion'].astype('str').str.split(':').str[0]

    return df



def crea_variable_target(df):
    """
    Esta funcion realiza crea la variable objetivo 'target' 
    """

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



def elimina_na_de_variable_delegacion(df):
    """ Esta funcion elimina los registros que tienen na para la varible 'delegacion_inicio',
      siempre y cuando el numero de registros a eliminar representen el 3% del total o menos """

    LIMIT_PERCENT = 0.03

    #Para el set de entrenamiento
    a_borrar = df[df['delegacion_inicio']=='na'].index
    if len(a_borrar) != 0:
       if len(a_borrar)/len(df) < LIMIT_PERCENT:
            df.drop(a_borrar , inplace=True)
       else:
            print("***** Los registros con 'na' son {}% *****\n*****No se borro nada *****".format( len(a_borrar)/len(df) ))

    return df





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





#Para Random Forest
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

    model=grid_search.fit(X_train, y_train)

    cv_results = pd.DataFrame(grid_search.cv_results_)
    results = cv_results.sort_values(by='rank_test_score', ascending=True)
    results.drop(['param_max_depth', 'param_max_features','param_min_samples_leaf','param_min_samples_split',
                  'param_n_estimators'], axis=1, inplace=True)
    results['modelo'] = 'random_forest'

    return results, model




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
                               cv = 10, 
                               n_jobs = -1)
                               #verbose = 3)

    model=grid_search.fit(X_train, y_train)

    cv_results = pd.DataFrame(grid_search.cv_results_)
    results = cv_results.sort_values(by='rank_test_score', ascending=True)
    results.drop(['param_C', 'param_penalty'], axis=1, inplace=True)
    results['modelo'] = 'regresion_log'

    return results, model



#Para XGboost
def magic_loop_XGB(X_train,y_train, hyper_params_grid):
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
                               cv = 10, 
                               n_jobs = -1)
                               #verbose = 3)

    model=grid_search.fit(X_train, y_train)

    cv_results = pd.DataFrame(grid_search.cv_results_)
    results = cv_results.sort_values(by='rank_test_score', ascending=True)
    results.drop(['param_n_estimators', 'param_learning_rate','param_subsample',
                 'param_max_depth'], axis=1, inplace=True)
    results['modelo'] = 'xgboost'

    return results, model





def completa_metadatos_modelo(meta, fname):

    #Metadatos referentes al usuario y fecha de ejecucion
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    date_time = datetime.datetime.now()
    fecha_de_ejecucion = date_time.strftime("%d/%m/%Y %H:%M:%S")
    ip_address = ip_address
    usuario = getpass.getuser()

    otros_meta = pd.DataFrame({'fecha_de_ejecucion':fecha_de_ejecucion, 
                               'ip_address': ip_address,
                               'usuario': usuario,
                               'archivo_modelo':fname+'.pkl',
                               'archivo_metadatos': 'metadata_'+fname+'.csv'}, index=[0])


    metadata = pd.concat([otros_meta, meta], axis=1, sort=False)

    return metadata



def metadata_modelo(model_name, y_test, y_tag, model_params):
       """ Esta funcion guarda los metadatos para el mejor modelo y los presenta en pantalla"""

       precision = precision_score(y_test.values.ravel(), y_tag)
       #Metricas para el modelo
       print("\n***********  ◦°˚\(*❛‿❛)/˚°◦   ********************")
       print("Modelo:",  model_name)
       print('Precision:', precision)
       ########## Confusion matrix
       conf_mat = pd.DataFrame(confusion_matrix(y_test.values.ravel(), y_tag))
       conf_mat = conf_mat[conf_mat.columns[::-1]].iloc[::-1]
       print(conf_mat)
       print('**************************************************\n')

       meta = pd.DataFrame({'precision': precision,
                            'parametros': model_params} ,index=[0])

       metadata = completa_metadatos_modelo(meta, model_name)

       return metadata




def hace_df_para_ys(y_proba, y_tag, y_test):
     """Esta funcion hace un df con las probas y etiquetas de las predicciones """ 
     vars = {'y_proba_0':y_proba[:,0],'y_proba_1':y_proba[:,1],'y_etiqueta':y_tag,'y_test':y_test.values.ravel()}
     df = pd.DataFrame(vars)
     return df




def dummies_a_var_categorica(df, dummies_names):
    """Esta funcion reconstruye la variable categorica de sus variables dummies
        param: df el dataframe que contiene las variables dummies
        param: dummies name lista de strings que contiene el nombre de las vars categoricas
        return: df_sin_dummies"""

    df_sin_dummies = df
    for dummy in dummies_names:
        aux = df.loc[:, df.columns.str.startswith(dummy)]
        aux = aux.idxmax(axis=1)
        aux = aux.str.replace(dummy+'_', '')

        #elimino las dummies
        cols_a_eliminar = df.columns[df.columns.str.startswith(dummy)].tolist()
        df_sin_dummies.drop(columns=cols_a_eliminar, inplace=True)
        #anado la var categorica
        df_sin_dummies = pd.concat([df_sin_dummies, aux.rename(dummy)], axis=1)

    return df_sin_dummies
