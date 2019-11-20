# -*- coding: utf-8 -*-
"""
Created on Mon Jul 23 09:01:55 2018

@author: michael.schulte
"""

import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go

from ab_test_evaluator import DashDataHelper

helper = DashDataHelper()

app = dash.Dash()

# app.config.supress_callback_exceptions = True

app.layout = html.Div([
                    html.H1(children = 'A/B Test Results Analyzer', 
                            style = {'width':'80%', 'text-align':'center', 'font-size':'18', 'font-family':'Arial Black'}),
                    
                    html.H2(children = 'Cool Tool; Boring Name',
                            style = {'width':'80%', 'text-align':'center', 'font-color':'gray',
                                     'font-size':'12', 'font-family':'Arial'}),

                    html.Div([
                            dcc.Dropdown(id = 'test_dropdown',
                                         placeholder = 'Select a test...')
                            ], 
                            style = {'width':'25%', 'height':'5%', 'display':'inline-block', 
                                     'font-size':'14', 'font-family':'Arial'}),

                    html.Div([
                            dcc.Dropdown(id = 'metric_dropdown',
                                         value = 'GROSS_REV',
                                         placeholder = 'Select a metric...')
                            ], 
                            style = {'width':'55%', 'height':'5%', 'display':'inline-block', 
                                     'font-size':'14', 'font-family':'Arial'}),

                    html.Div([
                            dcc.Textarea(id = 'start_dt',
                                         contentEditable = False,
                                         wrap = True,
                                         draggable = False,
                                         disabled = True,
                                         style = {'background-color':'#f1f3f4', 'border-color':'#f1f3f4', 
                                                  'text-align':'center', 'font-family':'Arial'})
                            ],
                            style = {'width':'8%', 'height':'5%', 'display':'inline-block', 'font-family':'Arial'}),

                    html.Div([
                            dcc.Graph(id = 'metrics_viz')
                            ], 
                            style = {'width':'90%','height':'30%','display':'inline-block',
                                     'text-align':'center', 'font-family':'Arial'}),

                    html.Div([
                            dcc.Graph(id = 'p-value_viz')
                            ], 
                            style = {'width':'44.5%','height':'30%','display':'inline-block', 'font-family':'Arial'}),

                    html.Div([
                            dcc.Graph(id = 'ci_viz')
                            ], 
                            style = {'width':'44.5%','height':'30%','display':'inline-block', 'padding-left':'1%', 'font-family':'Arial'}),

#                    html.Div([
#                            dcc.Graph(id = 'table')
#                            ], 
#                            style = {'width':'80%','height':'30%','display':'inline-block', 'padding-left':'1%'}),
                    
                    html.Div([
                            dcc.Markdown(id = 'test_description')
                            ], 
                            style = {'width':'80%','height':'30%','display':'inline-block', 
                                     'padding-left':'1%', 'font-size':'18', 'font-family':'Arial'}),
                            
                    ], 
                    style = {'padding-left':'5%', 'padding-right':'5%', 'padding-top':'0%',
                             'background-color':'#f1f3f4', 'font-family':'Arial'})

@app.callback(
        Output('test_dropdown','options'),
        [Input('metrics_viz','style')])
def test_list(a):
    '''Get most up-to-date list of tests for test_dropdown'''
    helper = DashDataHelper()
    test_list = helper.get_active_test_list()['test_name']
    
    return [{'label': i.replace('_',' '), 'value':i} for i in test_list]


@app.callback(
        Output('metric_dropdown','options'),
        [Input('test_dropdown','value')])
def test_list(test_name):
    '''Get metrics from selected test & populate in metric dropdown'''
    helper = DashDataHelper()
    cols = helper.get_daily_rollup(test_name).columns
    
    return [{'label': i.title().replace('_',' '), 'value':i} for i in cols]


@app.callback(
        Output('start_dt','value'),
        [Input('test_dropdown','value')])
def get_start_date(test):
    helper = DashDataHelper()
    df = helper.get_daily_rollup(test)
    
    dt = df['DT'].min().strftime('%m/%d/%Y')
    
    return 'Start Date: \n {}'.format(dt)

@app.callback(
        Output('metrics_viz', 'figure'),
        [Input('test_dropdown','value'),
         Input('metric_dropdown','value')])
def daily_metric(test_dropdown, metric_dropdown):
    '''Get selected test data & visualize selected metric'''
    helper = DashDataHelper()
    df = helper.get_daily_rollup(test_dropdown)
    
    trace1 = go.Scatter(x = df.loc[df['TEST_CELL'] == df['TEST_CELL'].sort_values().unique()[0]]['DT'], 
                        y = df.loc[df['TEST_CELL'] == df['TEST_CELL'].sort_values().unique()[0]][metric_dropdown], 
                        line = dict(color = ('#9A9EAB')),
                        name = df['TEST_CELL'].unique()[0])
    trace2 = go.Scatter(x = df.loc[df['TEST_CELL'] == df['TEST_CELL'].unique()[1]]['DT'], 
                        y = df.loc[df['TEST_CELL'] == df['TEST_CELL'].unique()[1]][metric_dropdown], 
                        line = dict(color = ('#EC96A4')),
                        name = df['TEST_CELL'].unique()[1])
    
    layout = go.Layout(yaxis = {'hoverformat':'.3f'},
                       title = metric_dropdown.title().replace('_',' '),)
#                       paper_bgcolor = '#e3e7ea')
    
    return {'data': [trace1,trace2], 'layout':layout}


@app.callback(
        Output('p-value_viz', 'figure'),
        [Input('test_dropdown','value'),
         Input('metric_dropdown','value')])
def p_val_chart(test_dropdown, metric_dropdown):
    '''Display P-Value Trends'''
    helper = DashDataHelper()
    df = helper.get_rolling_stats(test_dropdown)
    
    df = df.loc[df['METRIC_NAME'] == metric_dropdown]
    
    trace1 = go.Scatter(x = df.loc[df['TEST_CELL'] == df['TEST_CELL'].sort_values().unique()[0]]['DT'], 
                        y = df.loc[df['TEST_CELL'] == df['TEST_CELL'].sort_values().unique()[0]]['P_VALUE'],
                        line = dict(color=('#5D535E'))
                        )
    
    layout = go.Layout(title = 'Significance (P-Value)',
                       shapes = [{'type': 'line',
                                'x0': df['DT'].min(),
                                'y0': .05,
                                'x1': df['DT'].max(),
                                'y1': .05,
                                'line': {
                                    'color': '#DFE166',
                                    'width': 3,
                                    'dash': 'dash'}}],
                        yaxis = dict(hoverformat = '.3f',
                                     range=[0,1])
                        )
    
    return {'data': [trace1], 'layout':layout}


@app.callback(
        Output('ci_viz', 'figure'),
        [Input('test_dropdown','value'),
         Input('metric_dropdown','value')])
def ci_chart(test_dropdown, metric_dropdown):
    '''Display Metric Avg & CI'''
    helper = DashDataHelper()
    df = helper.get_rolling_stats(test_dropdown)
    
    df = df.loc[df['METRIC_NAME'] == metric_dropdown]
    
    y_val = df.loc[df['DT'] == df['DT'].max()]['METRIC_VALUE']
    
    trace1 = go.Bar(x = df['TEST_CELL'], 
                    y = y_val,
#                    text = ['{:.3f}'.format(i) for i in y_val],
#                    textposition = 'auto',
                    marker = dict(color = ['#9A9EAB', '#EC96A4']),
                    error_y = dict(type = 'data',
                                   symmetric = False,
                                   array = df.loc[df['DT'] == df['DT'].max()]['UPPER_CI'] - df.loc[df['DT'] == df['DT'].max()]['METRIC_VALUE'],
                                   arrayminus = df.loc[df['DT'] == df['DT'].max()]['METRIC_VALUE'] - df.loc[df['DT'] == df['DT'].max()]['LOWER_CI']
                                   )
                    )
    
    layout = go.Layout(title = 'Avg Performance To-Date',
                       yaxis=dict(range=[y_val.min() * .6, y_val.max() * 1.3],
                                  hoverformat = '.3f')
                       )

    return {'data': [trace1], 'layout':layout}


@app.callback(
        Output('table', 'figure'),
        [Input('test_dropdown','value')])
def metric_table(test_dropdown):
    '''NOT WORKING - should populate a table of metrics'''
    helper = DashDataHelper()
    df = helper.get_rolling_stats(test_dropdown)
    df = df.loc[df['DT'] == df['DT'].max()]
    piv = df.pivot(index = 'TEST_CELL', columns = 'METRIC_NAME', values = 'METRIC_VALUES').reset_index()
    
    cols = [[i.title().replace('_',' ')] for i in piv.columns]
    
    trace1 = go.Table(header = dict(values = [1,2]),
#                      cells = dict(values = ['a','b'])
                      )
                      
#                     header =  dict(values = (df['METRICS'])),
#                      cells = dict(values = (df['METRIC_VALUES']))
#                      )
    
    return {'data':[trace1]}


@app.callback(
        Output('test_description', 'children'),
        [Input('test_dropdown','value')])
def get_test_description(test_dropdown):
    helper = DashDataHelper()
    df = helper.get_active_test_list()
    
    df = df.loc[df['test_name'] == test_dropdown]
    
    return '### Test Description: \n' + df['description'].iloc[0].strip('\n')

#width = 1200, height = 300, plot_bgcolor = '#c7c7c7', paper_bgcolor = '#c7c7c7')


if __name__ == '__main__':
    app.run_server(debug = True, host='0.0.0.0')
