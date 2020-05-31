import dash_html_components as html
import pandas as pd
import dash
import dash_table
import pandas as pd
import dash_core_components as dcc
from flask import Flask
import plotly.graph_objs as go


df = pd.read_csv('predicciones_modelo.csv', sep=",")

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
    html.H1(children='Número de etiquetas positivas vs delegacion',
           style={
            'textAlign': 'left',
            'color': colors['text']
            }
           
           ),
            
    
    html.Div(
                [
                    html.H3("Graph 1"),
                    dcc.Graph(
                        id="g1",
                        figure={
                            "data": [
                                {'x': df['delegacion'], 'y': df['etiqueta'], 'type': 'bar', 'name':'live'},
                                {'x': df['delegacion'], 'y': df['etiqueta'], 'type': 'bar', 'name': 'test'},
            
                            ]
                        }
                    )
                ],
     ),
    ])
])

if __name__ == '__main__':
    app.run_server(debug=True)
    
    
