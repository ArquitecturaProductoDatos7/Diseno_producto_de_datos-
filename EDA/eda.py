#Este módulo contiene todas las funciones utilizadas para generar el reporte EDA, GDA

import seaborn as sns
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def tabla_estadisticos_descriptivos_variables_numericas(archivo):
    def selecciona_variables_numericas(archivo):
        lista_variables_numericas=archivo.select_dtypes(include = 'number').columns.values
        archivo_variables_numericas = archivo.loc[:, lista_variables_numericas]
        return archivo_variables_numericas
    def q25(archivo):
        qal25=archivo.quantile(.25)
        return qal25
    def q75(archivo):
        qal75=archivo.quantile(.75)
        return qal75
    def uniques(archivo):
        uni=archivo.nunique()
        return uni
    def missings(archivo):
        nulos=archivo.isnull().sum()
        return nulos
    def prop_missings(archivo):
        prop=(archivo.isnull().sum()/len(archivo.index))*100
        return prop
    archivonum=selecciona_variables_numericas(archivo).agg(['max','min','mean','std',q25,'median',q75,'skew','kurt','count',uniques,missings,prop_missings])
    return archivonum

def tabla_estadisticos_descriptivos_variables_categoricas(archivo):
    def selecciona_variables_categoricas(archivo):
        lista_variables_categoricas=archivo.select_dtypes(include = 'object').columns.values
        archivo_variables_categoricas = archivo.loc[:, lista_variables_categoricas]
        return archivo_variables_categoricas
    def uniques(archivo):
        uni=archivo.nunique()
        return uni
    def uniques_list(archivo):
        uni=archivo.unique()
        return uni
    def missings(archivo):
        nulos=archivo.isnull().sum()
        return nulos
    def prop_missings(archivo):
        prop=(archivo.isnull().sum()/len(archivo.index))*100
        return prop
    archivocat=selecciona_variables_categoricas(archivo).agg(['count',uniques,uniques_list,missings,prop_missings])
    return archivocat

def grafico_histograma_categorica(archivo,variable_categorica,variable_contar,etiqueta_ejex,etiqueta_ejey,titulo):
    result=archivo.groupby([variable_categorica])[variable_contar].count().reset_index().sort_values(variable_contar,ascending=False)
    graf=sns.countplot(x=variable_categorica, palette="Paired", data=archivo,order=result[variable_categorica].values);
    graf.set_xticklabels(graf.get_xticklabels(), rotation=45, ha="right")
    graf.set(xlabel=etiqueta_ejex, ylabel=etiqueta_ejey, title=titulo)    

def grafico_barplot_orden_decreciente(archivo,variable_ejex,variable_ejey,etiqueta_ejex,etiqueta_ejey,titulo):
    result = archivo.groupby([variable_ejex])[variable_ejey].mean().reset_index().sort_values(variable_ejey,ascending=False)
    graf=sns.barplot(x=variable_ejex, y=variable_ejey, data=archivo, order=result[variable_ejex])
    graf.set_xticklabels(graf.get_xticklabels(), rotation=40, ha="right")
    graf.set(xlabel=etiqueta_ejex, ylabel=etiqueta_ejey, title=titulo)

def tabla_ranking_n(archivo,n,categoria1,categoria2,variablenumerica):
    tabla=archivo.pivot_table(index=[categoria1,categoria2],values=[variablenumerica],aggfunc=np.mean)
    topn=tabla.sort_values(by=variablenumerica,ascending=False).head(n)
    return topn    
    
def grafico_barplot_orden_en_barras(archivo,variable_ejex,variable_ejey,lista_orden_barras,etiqueta_ejex,etiqueta_ejey,titulo):
    graf=sns.catplot(x=variable_ejex, y=variable_ejey,order=lista_orden_barras,
                kind="bar",data=archivo)
    graf.set(xlabel=etiqueta_ejex, ylabel=etiqueta_ejey, title=titulo)
            
    
def grafico_strip(archivo,variable_ejex,variable_ejey,etiqueta_ejex,etiqueta_ejey,titulo):
    graf=sns.catplot(x=variable_ejex, y=variable_ejey,kind="strip", dodge=False, data=archivo, palette="pastel")
    graf.set(xlabel=etiqueta_ejex, ylabel=etiqueta_ejey, title=titulo)
        
def matriz_correlacion(archivo):
    plt.figure(figsize=(6,6))
    sns.heatmap(archivo.corr(),vmin=-1,cmap='RdYlGn');
