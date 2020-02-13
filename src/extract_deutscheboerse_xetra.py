import requests
import pandas as pd
import datetime
import xlrd
from toolbox import append_df_to_excel

api_host = 'http://api.developer.deutsche-boerse.com/prod/xetra-public-data-set/1.0.0/'
api_key = 'INSERT_API_KEY from https://console.developer.deutsche-boerse.com/projects'
function_headers = {'X-DBP-APIKEY': api_key}

market = 'xetra'
#market = 'eurex'

project_space_dir = r'C:\dev\git\portfolio_analyzer'

excel_file_in = project_space_dir+r'\data\in.xlsx'
excel_file_out= project_space_dir+r'\data\result.xlsx'

###load transaction list###
transactions = pd.read_excel (excel_file_in,
                              sheet_name = 'transactions',
                              header=0)

###use prev. day in order to get end of day prices###
analysis_date = datetime.date.today() - datetime.timedelta(days=1)
###use last friday if weekend###
if analysis_date.weekday() == 6:
    analysis_date = analysis_date - datetime.timedelta(days=2)
elif analysis_date.weekday() == 5:
    analysis_date = analysis_date - datetime.timedelta(days=1)
analysis_date_str = str(analysis_date)
#analysis_date_str = '2020-02-07'

###only start process, if data is not yet persisted in excel###
exists = 0
try:
    exists = xlrd.open_workbook(excel_file_out).sheet_by_name('overview_'+analysis_date_str).nrows
except Exception as error:
    print(error)
if(exists>0):
    print('data for '+analysis_date_str+' allready analyzed and result is persisted in excel')
else:
    ###load unique isins###
    isins = transactions['isin'].unique()
    isins_df = pd.DataFrame(isins, columns = ['isin'])
    isins_df['current_date'] = 'NaN'
    isins_df['current_time'] = 'NaN'
    isins_df['current_price'] = float('NaN')

    print('\n pre rest call')
    print(isins_df)


    print('\n execute rest calls')
    ###load current market data for isins###
    for index, row in isins_df.iterrows():
        isin = row['isin']
        function_url = api_host+market+'?'\
                       +'isin'+'='+isin\
                       +'&'+'date'+'='+analysis_date_str
        ###actual rest call###
        function_response = requests.get(function_url, headers=function_headers)
        print(str(function_response) + ' - ' + function_url)
        if function_response.status_code != 200:
            print(function_response.content)

        ###cast to pandas dataframe###
        function_response_json = function_response.json()
        function_response_df = pd.DataFrame(function_response_json, columns = ['Isin', 'Mnemonic', 'SecurityDesc',
                                                                               'SecurityType', 'Currency', 'SecurityID',
                                                                               'Date', 'Time', 'StartPrice', 'MaxPrice',
                                                                               'MinPrice', 'EndPrice', 'TradedVolume',
                                                                               'NumberOfTrades'])
        current_price = -1
        ###load latest###
        for inner_index, inner_row in function_response_df.tail(1).iterrows():
            #print(inner_row)
            current_price = inner_row['EndPrice']
            current_date = inner_row['Date']
            current_time = inner_row['Time']
        ###save to dataframe###
        isins_df.at[index, 'current_date'] = current_date
        isins_df.at[index, 'current_time'] = current_time
        isins_df.at[index, 'current_price'] = current_price


    print('\n post rest call')
    print(isins_df)

    result = pd.merge(transactions, isins_df, on='isin')

    ###generate new columns###
    result['number'] = round(result['buy_volume'] / result['buy_price'], 2)
    result['current_volume'] = round(result['number'] * result['current_price'], 2)
    result['buy_date'] = result['buy_date'].astype('datetime64[h]')
    result['current_date'] = result['current_date'].astype('datetime64[h]')
    result['holding_period'] = (result['current_date']-result['buy_date'])

    ###filter out "empty" rows and forbid timetravel###
    result = result[result.buy_price.notnull()]
    result = result[result.holding_period >= datetime.timedelta(days=0)]

    ###write result###
    ###overall###
    sheet_exists = 0
    try:
        sheet_exists = xlrd.open_workbook(excel_file_out).sheet_by_name('overview').nrows
    except Exception as error:
        print(error)
    if (sheet_exists == 0):
        append_df_to_excel(excel_file_out, result,
                           sheet_name='overview',
                           header=True,
                           truncate_sheet=True,
                           index=False)
    else:
        append_df_to_excel(excel_file_out, result,
                           sheet_name='overview',
                           header=False,
                           truncate_sheet=False,
                           index=False)
    ###analysis_date###
    append_df_to_excel(excel_file_out, result,
                       sheet_name='overview_'+analysis_date_str,
                       truncate_sheet=True,
                       index=False)