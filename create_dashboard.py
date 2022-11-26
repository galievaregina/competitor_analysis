from connection_with_postgres import get_diff_price
from dash import Dash, html, dcc
import plotly.express as px
import pandas as pd


app = Dash(__name__)

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options
hostkey = get_diff_price('20_11_2022', '22_11_2022','hostkey')
reg_ru = get_diff_price('20_11_2022', '22_11_2022','reg_ru')
timeweb = get_diff_price('20_11_2022', '22_11_2022','timeweb')
servers_ru = get_diff_price('20_11_2022', '22_11_2022','servers_ru')

fig_hostkey = px.line(hostkey, x='date', y='diff')
fig_reg_ru = px.line(reg_ru, x='date', y='diff')
fig_timeweb = px.line(timeweb, x='date', y='diff')
fig_servers_ru = px.line(servers_ru, x='date', y='diff')

app.layout = html.Div(children=[
    html.H1(children='Разница - цена за предыдущий день/цена за этот день'),
    html.H1(children='Hostkey'),

    dcc.Graph(
        id='example-graph',
        figure=fig_hostkey
    ),
    html.H1(children='Reg.ru'),

    dcc.Graph(
        id='example-graph',
        figure=fig_reg_ru
    ),
    html.H1(children='Timeweb'),

    dcc.Graph(
        id='example-graph',
        figure=fig_timeweb
    ),

    html.H1(children='Servers.ru'),

    dcc.Graph(
        id='example-graph',
        figure=fig_servers_ru
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)
