# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

#import pandas as pd
#import numpy as np

import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
from pandas_datareader import data as web
#from datetime import datetime as dt
#import plotly.plotly as py
import plotly.graph_objs as go
#import plotly as pl


app = dash.Dash()

app.layout = html.Div([
                html.H1(children = 'viz'),
                
                dcc.Dropdown(id = 'dropdown',
                        options = [
                         {'label': 'Tesla', 'value':'TSLA'},
                         {'label': 'Apple', 'value':'AAPL'}],
                         value = 'TSLA'),
                dcc.Graph(id = 'viz')
                    ])

@app.callback(
        Output('viz', 'figure'),
        [Input('dropdown','value')])
def get_ticker(abc):
    df = web.DataReader(abc, data_source = 'robinhood')
    df = df.reset_index()
    
    trace = [go.Scatter(x = df['begins_at'], y = df['close_price'], mode = 'line', name = abc)]
    
    return {'data':trace}


if __name__ == '__main__':
    app.run_server(debug = True)


#from ab_testing_import.dash_data_helper import DashDataHelper

#helper = DashDataHelper()
#helper.get_active_test_list() # returns DataFrame with TEST_NAME column
#helper.get_daily_rollup(test_name) # returns DataFrame of the daily rollup table for the test
#helper.get_rolling_stats(test_name) # not implemented yet; waiting on Klaus + Deemah