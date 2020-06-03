# config: utf8 
from flask import Flask, request
#import werkzeug                                           #Para la ec2
#werkzeug.cached_property = werkzeug.utils.cached_property #Para la ec2
from flask_restplus import Api, Resource
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, and_

app=Flask(__name__)
#app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres:passwordDB@db-dpa.c2eodp3soooc.us-east-1.rds.amazonaws.com:5432/db_accidentes_cdmx"
# se define el engine de la base de datos de postgresql
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres:passwordDB@db-dpa20.clkxxfkka82h.us-east-1.rds.amazonaws.com:5432/db_incidentes_cdmx"
db = SQLAlchemy(app)
#para modificar el título de la API y su descripción en la documentación
api=Api(app, title= 'API PREDICCIONES', description= 'API que muestra las variables introducidas por el usuario, las probabilidades y etiqueta asociadas a esa observación', default='Predicciones', default_label='- - -')


#clase que define la tabla y columnas en la que se van a realizar las consultas
class TablaPredicciones(db.Model):

    #nombre y esquema de la tabla
    __tablename__= "predicciones"
    __table_args__ = {"schema":"prediccion"}

    #columnas de la tabla
    id= db.Column(db.Integer(), primary_key=True) #la tabla debe tener un id
    mes = db.Column(db.Integer())
    hora = db.Column(db.Integer())
    delegacion_inicio = db.Column(db.String())
    dia_semana = db.Column(db.String())
    tipo_entrada = db.Column(db.String())
    incidente_c4_rec= db.Column(db.String())
    ano= db.Column(db.Integer())
    y_proba_0= db.Column(db.Float())
    y_proba_1= db.Column(db.Float())
    y_etiqueta= db.Column(db.Integer())

    #las variables que se van a exponer
    def __repr__(self):

        return "<predicciones(y_proba_0='{}', y_proba_1='{}', y_etiqueta='{}')>"\
                .format(self.y_proba_0,self.y_proba_1,self.y_etiqueta)


#se definen las variables que va a meter el usuario
@api.route("/prediccion/<int:mes>/<int:hora>/<delegacion>/<dia_semana>/<tipo_entrada>/<tipo_incidente>")
@api.doc(responses={404: 'Prediccion no encontrada', 200:'Prediccion encontrada'},params={'mes':'Especifica el número del mes (1-12)','hora':'Especifica la hora sin minutos en formato 24 horas','delegacion': 'Especifica la delegación sin acentos y en minúsculas','dia_semana':'Especifica el día de la semana sin acentos y en minúsculas', 'tipo_entrada':'Especifica el tipo de entrada sin acentos y en minúsculas', 'tipo_incidente': 'Especifica el tipo de incidente: accidente, detencion ciudadana, cadaver, lesionado, sismo'})
class ExponerPredicciones(Resource):

    #metodo get
    def get(self,mes,hora,delegacion,dia_semana,tipo_entrada,tipo_incidente):
        """
        Endpoint para mostrar las predicciones de acuerdo al valor de las variables introducidas
        """
        records = TablaPredicciones.query.filter(
                                    and_(TablaPredicciones.mes==mes,
                                         TablaPredicciones.hora==hora,
                                         TablaPredicciones.delegacion_inicio==str(delegacion),
                                         TablaPredicciones.dia_semana==str(dia_semana),
                                         TablaPredicciones.tipo_entrada==str(tipo_entrada),
                                         TablaPredicciones.incidente_c4_rec==str(tipo_incidente))).limit(1).all()

        results = [
            {
                "mes": record.mes,
                "hora": record.hora,
                "delegacion": record.delegacion_inicio,
                "dia_semana": record.dia_semana,
                "tipo_entrada": record.tipo_entrada,
                "tipo_incidente":record.incidente_c4_rec,
                "proba_0": record.y_proba_0,
                "proba_1": record.y_proba_1,
                "etiqueta": record.y_etiqueta

            } for record in records]

        if not results:
            api.abort(404, 'Prediccion no encontrada')
        else:
            return {"observaciones": len(results), 
                    "resultados": results}



@api.route("/predicciones/<int:ano>")
@api.doc(responses={404: 'Prediccion no encontrada', 200:'Prediccion encontrada'},params={'ano':'Especifica el año'})
class ExponerPrediccionesAnual(Resource):
    
     #metodo get
    def get(self,ano):
        """
        Endpoint para mostrar las predicciones de acuerdo al año introducido
        """
        records = TablaPredicciones.query.filter(
                                    and_(TablaPredicciones.ano==ano,
                                         TablaPredicciones.mes==4)).all()
        results = [
            {
                "mes": record.mes,
                "hora": record.hora,
                "delegacion": record.delegacion_inicio,
                "dia_semana": record.dia_semana,
                "tipo_entrada": record.tipo_entrada,
                "tipo_incidente":record.incidente_c4_rec,
                "proba_0": record.y_proba_0,
                "proba_1": record.y_proba_1,
                "etiqueta": record.y_etiqueta
                
            } for record in records]
        
        if not results:
            api.abort(404, 'Prediccion no encontrada')
        else:
            return {"observaciones": len(results), 
                    "resultados": results}


if __name__ == '__main__':
    app.run()
