import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_table
import pandas as pd
#from flask import Flask
import plotly.graph_objs as go
import plotly.express as px
from dash.dependencies import Input, Output


df = pd.read_csv('predicciones_modelo.csv', sep="\t")
df_mensual = pd.read_csv('predicciones_mes_4_ano_2020.csv', sep="\t", header=None)

app = dash.Dash(__name__)

#server = Flask(__name__)
#app = dash.Dash(server=server,  meta_tags=[{"name": "viewport", "content": "width=device-width"}])
#server = app.server



#app.title = 'dashboard para monitorear el desempeño del modelo'

colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}
#style={'backgroundColor': colors['background']},
app.layout = html.Div(children=[
    html.H1(
        children='Dashboard para monitoreo del modelo',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),
    
    html.Div([
    dash_table.DataTable(
    id='table',
    columns=[{"name": i, "id": i} for i in df.columns],
    data=df.to_dict('records'),
    #style_cell = {"fontFamily": "Arial", "size": 10, 'textAlign': 'left'},
    selected_rows=[0],
    style_table={
                'maxHeight': '50ex',
                'overflowY': 'scroll',
                'width': '50%',
                'minWidth': '100%',
            },
    style_cell={
                'fontFamily': 'Open Sans',
                'textAlign': 'center',
                'height': '15px',
                'padding': '2px 22px',
                'whiteSpace': 'inherit',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
    #row_selectable='multi',
    ),
    #dcc.Graph(id='table-editing-simple-output')

    ], className="twelve columns"),
    
    html.Div(children=[
    html.H1(children='Gráficas de entrenamiento vs producción',
           style={
            'textAlign': 'left',
            'color': colors['text']
            }
           
           ),
            
    
    html.Div(
                [
                    html.H3("Proporción de etiquetas"),
                    dcc.Graph(
                        id="g1",
                        figure={
                            "data": [
                                {'x': [0,1], 'y': [(len(df_mensual[df_mensual['etiqueta'] == 0])/len(df_mensual.index)),(len(df_mensual[df_mensual['etiqueta'] == 1])/len(df_mensual.index))], 'type': 'bar', 'name':'live'},
                                {'x': [0,1], 'y': [(len(df[df['etiqueta'] == 0])/len(df.index)),(len(df[df['etiqueta'] == 1])/len(df.index))], 'type': 'bar', 'name': 'train'},
            
                            ],
                            "layout": {
                                'xaxis':{'title':'etiqueta'},
                                'yaxis':{'title':'proporción'}
                            }
                        }
                    )
                ],
     ),
    ])
])

########### ************************************
predicciones_modelo = pd.read_csv('predicciones_modelo.csv', sep = '\t')
predicciones_modelo = pd.DataFrame(predicciones_modelo)
predicciones_mensual = pd.read_csv('predicciones_mes_4_ano_2020.csv', sep="\t", header=None)

predicciones_modelo.columns = ['mes', 'hora', 'delegacion', 'dia_semana', 'tipo_entrada', 'incidente', 'ano', 'y_proba_0', 'y_proba_1', 'y_etiqueta']
predicciones_mensual.columns = ['mes', 'hora', 'delegacion', 'dia_semana', 'tipo_entrada', 'incidente', 'ano', 'y_proba_0', 'y_proba_1', 'y_etiqueta']

predicciones_modelo.insert(0,'Datos', 'Historicos')
predicciones_mensual.insert(0,'Datos', 'Live')

df = pd.concat([predicciones_modelo, predicciones_mensual], axis=0)


# Dropdown menu por delegacion para graficar % de 1s
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
#Aqui va la info
df = pd.concat([predicciones_modelo, predicciones_mensual], axis=0)
available_indicators = df['delegacion'].unique()

app.layout = html.Div([
    html.Div([
        dcc.Dropdown(
            id='xaxis-delegacion',
            options=[{'label': i, 'value': i} for i in available_indicators],
            value='cuauhtemoc'
            ),
        dcc.RadioItems(
            id='xaxis-etiqueta',
            options=[{'label': i, 'value': i} for i in ['y_proba_1', 'y_proba_0']],
            value='y_proba_1',
            labelStyle={'display': 'inline-block'}
            )
    ]),
    dcc.Graph(id='histogram-graph')
],
style={'width': '48%', 'display': 'inline-block'}
)
    
   

@app.callback(
    Output('histogram-graph', 'figure'),
    [Input('xaxis-delegacion', 'value'),
     Input('xaxis-etiqueta', 'value')]
)
def update_graph(xaxis_delegacion, xaxis_etiqueta):
    dff = df[df['delegacion'] == xaxis_delegacion]
    
    fig = px.histogram(dff, x=dff[xaxis_etiqueta], color="Datos", histnorm='percent', nbins=50, barmode="overlay",
                   title='Comparación de las distribuciones <br> (Datos históricos vs. Live)',
                   labels={'y_proba_1': 'Probabilidad de etiqueta 1',
                           'y_proba_0': 'Probabilidad de etiqueta 0',
                           'percent': 'Porcentaje'}
                  )
    fig.update_yaxes(title_text='Porcentaje')
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
    
    
