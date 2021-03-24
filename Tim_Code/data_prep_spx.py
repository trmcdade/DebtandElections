import pandas as pd
import os
from os import listdir

'''
Source: yahoo finance: https://finance.yahoo.com/quote/%5EGSPC/history?period1=628300800&period2=1594080000&interval=1mo&filter=history&frequency=1mo
'''

spx = pd.read_csv('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/SPX/SPX_historical.csv')
spx['Date'] = [pd.to_datetime(x) for x in spx['Date']]
spx['Year'] = [pd.to_datetime(x).year for x in spx['Date']]
spx['Month'] = [pd.to_datetime(x).month for x in spx['Date']]
spx = spx.sort_values(by = ['Date'])
spx['Prev_Open'] = [spx.loc[0, 'Open']] + [spx.loc[x-1, 'Open'] for x in range(1, spx.shape[0])]
spx['Open_Diff'] = [spx.loc[x, 'Open'] - spx.loc[x, 'Prev_Open'] for x in range(spx.shape[0])]
spx['SPX_Pct_Change_MoM'] = [spx.loc[x, 'Open_Diff'] / spx.loc[x, 'Prev_Open'] for x in range(spx.shape[0])]
spx = spx[spx['Date'] >= pd.to_datetime('1990-01-01')]
spx = spx[['Year', 'Month', 'SPX_Pct_Change_MoM']]
spx.head()

spx.to_csv('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/SPX/SPX_historical_2020_TM_20200708.csv')
# outpath = '/Explanatory Vars/SPX/SPX_historical_2020_TM_20200708.csv'
