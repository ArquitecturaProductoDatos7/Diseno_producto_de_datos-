from aequitas.group import Group
from aequitas.bias import Bias
import socket   #para ip de metadatos
import getpass  #para el usuario
import datetime
import pandas as pd

def MetricasBiasFairness(df_bias):
    """
       Esta funcion calcula las métricas  de interes para el producto de datos en un data frame:
       a)FOR- False Omission Rate
       b)FNR-False Negative Rate
    """
    
    #Se arma el df que requiere aequitas
    df =df_bias[['delegacion_inicio','y_etiqueta','y_test']]
    #Renombra columnas
    df=df.rename(columns={'y_etiqueta': 'score',
                            'y_test': 'label_value'})
    #Asigna tipo apropiado de variable
    df[df.columns.difference(['label_value', 'score'])] = df[
    df.columns.difference(['label_value', 'score'])].astype(str)
    
    g = Group()
    xtab, _ = g.get_crosstabs(df)
    
    #Disparidad en relación a un grupo especifico del atributo protegido
    b = Bias()
    bdf = b.get_disparity_predefined_groups(xtab, original_df=df, 
        ref_groups_dict={'delegacion_inicio':'iztapalapa'},alpha=0.05, mask_significance=True)
    bdf.style
    df_aequitas = bdf.style.data[['attribute_value','for','fnr','for_disparity','fnr_disparity']]
    idx_max_for = df_aequitas['for'].idxmax()
    delegacion_max_for=df_aequitas.attribute_value[idx_max_for]
    idx_max_fnr = df_aequitas['fnr'].idxmax()
    delegacion_max_fnr=df_aequitas.attribute_value[idx_max_fnr]
    idx_min_for = df_aequitas['for'].idxmin()
    delegacion_min_for=df_aequitas.attribute_value[idx_min_for]
    idx_min_fnr = df_aequitas['fnr'].idxmin()
    delegacion_min_fnr=df_aequitas.attribute_value[idx_min_fnr]
    prom_for=df_aequitas['for'].mean()
    prom_fnr=df_aequitas['fnr'].mean()
    metadata_bias = {'delegacion_max_FOR': [delegacion_max_for],
        'delegacion_max_FNR': [delegacion_max_fnr], 'promedio_FOR': [prom_for],'promedio_FNR':
                     [prom_fnr],'delegacion_min_FOR':[delegacion_min_for],
                    'delegacion_min_FNR':[delegacion_min_fnr]}

    df = pd.DataFrame(metadata_bias, columns = ['delegacion_max_FOR', 'delegacion_max_FNR',
                                               'promedio_FOR','promedio_FNR',
                                               'delegacion_min_FOR','delegacion_min_FNR'])

    return df_aequitas,df



def completa_metadatos_bias(meta, fname):

    #Metadatos referentes al usuario y fecha de ejecucion
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    date_time = datetime.datetime.now()
    fecha_de_ejecucion = date_time.strftime("%d/%m/%Y %H:%M:%S")
    ip_address = ip_address
    usuario = getpass.getuser()

    otros_meta = pd.DataFrame({'fecha_de_ejecucion':fecha_de_ejecucion, 
                               'ip_address': ip_address,
                               'usuario': usuario})


    metadata = pd.concat([otros_meta, meta], axis=1, sort=False)

    return metadata