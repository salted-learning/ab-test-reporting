# -*- coding: utf-8 -*-
"""
Created on Mon Jul 23 09:01:55 2018

@author: michael.schulte
"""
import base64
import io
import os
import time

import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go

import pandas as pd
import yaml

from ab_test_evaluator.dash_data_helper import DashDataHelper
from ab_test_evaluator import ExistingABTestFromDB, ABTest
from ab_test_evaluator.ab_test import _clean_dict

helper = DashDataHelper()

app = dash.Dash(__name__)

# app.config.supress_callback_exceptions = True

app.layout = html.Div([
                    html.H1(children = 'A/B Test Results Analyzer',
                            className='app-header'),
    
                    html.Div([html.Button('Add New Test',
                                          id='new-test-button')],
                             id='new-test-button-div',
                             className='test-selector'),
                    
                    html.H2(children = 'Cool Tool; Boring Name',
                            className='app-header'),

                    # holder for dropdown bar
                    html.Div([
                        html.Div([dcc.Dropdown(id='test_dropdown',
                                               placeholder='Select a test...')],
                                 id='test-dropdown-div',
                                 className='test-selector'),

                        html.Div([dcc.Dropdown(id='metric_dropdown',
                                               value='GROSS_REV',
                                               placeholder='Select a metric...')],
                                 id='metric-dropdown-div',
                                 className='test-selector'),

                        html.Div([html.Button('Refresh Test Data',
                                              id='refresh-button',
                                              className='test-selector')],
                                 id='refresh-button-div',
                                 className='test-selector'),
                    ],
                    id='dropdown-bar'
                    ),

                    # this isn't showing up anywhere?
                    html.Div([
                            dcc.Textarea(id = 'start_dt',
                                         contentEditable = False,
                                         wrap = True,
                                         draggable = False,
                                         disabled = True,
                                         style = {'background-color':'#f1f3f4', 'border-color':'#f1f3f4', 
                                                  'text-align':'center', 'font-family':'Arial'})
                            ]),

                    html.Div([
                            dcc.Graph(id = 'metrics_viz')
                            ],
                             id='metrics-viz-holder',
                             className='chart'),

                    html.Div([
                            dcc.Graph(id = 'p-value_viz')
                            ],
                             id='p-val-viz-holder',
                             className='chart'),

                    html.Div([
                            dcc.Graph(id = 'ci_viz')
                            ],
                             id='ci-viz-holder',
                             className='chart'),
                    
                    html.Div([
                            dcc.Markdown(id = 'test_description')
                            ],
                             id='description'),

                   html.Div([
                       html.Div([html.Span(children='x',
                                           id='close-refresh-modal',
                                           className='close'),
                                 html.Br(),
                                 html.H3(children="Upload our updated CSV file below, then wait as it's processed",
                                         className='upload-modal-title',
                                         id='refresh-modal-title'),
                                 dcc.Upload(id='upload-refreshed-csv',
                                            className='upload-csv',
                                            children='Drag and Drop or Click to Select Files'),
                                 html.Div([html.Div(id='upload-refreshed-csv-results')],
                                          id='upload-refreshed-csv-results-holder',
                                          className='upload-csv-results')],
                                className='modal-main',
                                id='refresh-modal-main')],
                            id='upload-refresh-modal',
                            className='modal-hidden'),

                   html.Div([
                       html.Div([html.Span(children='x',
                                           id='close-new-modal',
                                           className='close'),
                                 html.Br(),
                                 html.H3(children="Upload both your config file and your CSV file at the same time by selecting them both in the file browser.",
                                         className='upload-modal-title',
                                         id='new-modal-title'),
                                 dcc.Upload(id='upload-new-csv',
                                            multiple=True,
                                            className='upload-csv',
                                            children='Drag and Drop or Click to Select Files'),
                                 html.Div([html.Div(id='upload-new-csv-results')],
                                          id='upload-new-csv-results-holder',
                                          className='upload-csv-results')],
                                className='modal-main',
                                id='new-modal-main')],
                            id='upload-new-modal',
                            className='modal-hidden'),
    
                   html.Div(id='data-processing', style={'display': 'none'}),
                   html.Div(id='update-test-list', style={'display': 'none'})
    
],
                      id='main')


@app.callback(
        Output('test_dropdown', 'options'),
        [Input('update-test-list', 'children')])
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
        Output('test_description', 'children'),
        [Input('test_dropdown','value')])
def get_test_description(test_dropdown):
    helper = DashDataHelper()
    df = helper.get_active_test_list()
    
    df = df.loc[df['test_name'] == test_dropdown]
    
    return '### Test Description: \n' + df['description'].iloc[0].strip('\n')


@app.callback(
    Output('upload-refresh-modal', 'className'),
    [Input('refresh-button', 'n_clicks'), Input('close-refresh-modal', 'n_clicks')],
    [State('upload-refresh-modal', 'className')])
def upload_modal_state(open_clicks, close_clicks, class_name_state):
    if open_clicks == 0 or open_clicks is None: # initial load
        return 'modal-hidden'
    elif class_name_state == 'modal-hidden':
        return 'modal-visible'
    elif class_name_state == 'modal-visible':
        return 'modal-hidden'


@app.callback(
    [Output('upload-refreshed-csv-results', 'children'),
     Output('upload-refreshed-csv-results', 'className'),
     Output('close-refresh-modal', 'n_clicks')],
    [Input('upload-refreshed-csv', 'contents')],
    [State('upload-refreshed-csv', 'filename'),
     State('test_dropdown', 'value'),
     State('close-refresh-modal', 'n_clicks'),
     State('upload-refresh-modal', 'className')])
def update_test_csv(contents, filename, test_name, n_close_clicks, modal_state):
    if modal_state == 'modal-hidden': # do nothing
        return dash.no_update, dash.no_update, dash.no_update
    
    # WARNING: does not accept multiple files at this time
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        _, ext = os.path.splitext(filename)
        if ext == '.csv':
            df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
            test = ExistingABTestFromDB(df, test_name)
            # TODO: error handling, some sort of indication while the data is processing...
            # For the processing, this should be broken out into 2 functions using something
            # like example 1 here: https://dash.plot.ly/sharing-data-between-callbacks. This
            # method would update the hidden div and message section, then another callback
            # would look at the hidden div as the input and actually perform the refresh.
            test.refresh_test_data()
            return dash.no_update, '', n_close_clicks + 1
        else:
            return f'ERROR: Need a .csv file, not {ext}', 'error', dash.no_update
    except Exception as e:
        print(e)
        return f'There was an error processing the CSV file: {str(e)}', 'error', dash.no_update


@app.callback(
    Output('upload-new-modal', 'className'),
    [Input('new-test-button', 'n_clicks'), Input('close-new-modal', 'n_clicks')],
    [State('upload-new-modal', 'className')])
def new_modal_state(open_clicks, close_clicks, class_name_state):
    if open_clicks == 0 or open_clicks is None: # initial load
        return 'modal-hidden'
    elif class_name_state == 'modal-hidden':
        return 'modal-visible'
    elif class_name_state == 'modal-visible':
        return 'modal-hidden'


@app.callback(
    [Output('upload-new-csv-results', 'children'),
     Output('upload-new-csv-results', 'className'),
     Output('close-new-modal', 'n_clicks'),
     Output('update-test-list', 'children')],
    [Input('upload-new-csv', 'contents')],
    [State('upload-new-csv', 'filename'),
     State('close-new-modal', 'n_clicks'),
     State('upload-new-modal', 'className')])
def new_test_csv(contents_list, filename_list, n_close_clicks, modal_state):
    if modal_state =='modal-hidden': # do nothing
        return dash.no_update, dash.no_update, dash.no_update
    
    if not isinstance(n_close_clicks, int): # not sure what's the deal here
        n_close_clicks = 0
        
    try:
        # TODO: add error handling
        assert len(contents_list) == 2
        assert len(filename_list) == 2
        
        df = None
        config = None
        for i in range(len(contents_list)):
            _, ext = os.path.splitext(filename_list[i])
            content_type, content_string = contents_list[i].split(',')
            decoded = base64.b64decode(content_string)
            if ext == '.csv':
                df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
            elif ext == '.yml':
                config = yaml.safe_load(decoded)
                config = _clean_dict(config)

        assert df is not None
        assert config is not None

        test = ABTest(df, config)
        test.refresh_test_data()
        return '', '', n_close_clicks + 1, 'UPDATE'
    except Exception as e:
        print(e)
        return f'ERROR: {str(e)}', 'error', dash.no_update, dash.no_update
    
        

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', dev_tools_ui=False)
