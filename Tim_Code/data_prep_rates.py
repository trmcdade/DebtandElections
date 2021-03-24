import pandas as pd
import os
from os import listdir
import numpy as np
from tqdm import tqdm

# libor
l = pd.read_csv('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/Interest Rates/libor.csv')
l.columns = ['Date', 'LIBOR O/N']
l['Date'] = [pd.to_datetime(x) for x in l['Date']]
l['Month'] = [x.month for x in l['Date']]
l['Year'] = [x.year for x in l['Date']]
l = l.sort_values(by = ['Date'])
liborlist = []
for y in tqdm(l['Year'].unique()):
    y_df = l[l['Year'] == y]
    for m in y_df['Month'].unique():
        m_df = y_df[y_df['Month'] == m].reset_index(drop = True)
        ym = pd.DataFrame()
        ym.loc[0,'Year'] = y
        ym.loc[0,'Month'] = m
        min_date = min(m_df['Date'])
        ym.loc[0,'LIBOR O/N'] = m_df.loc[m_df['Date'] == min_date]['LIBOR O/N'].values[0]
        liborlist.append(ym)

libor = pd.concat(liborlist, ignore_index = True)
libor = libor[libor['Year'] >= 1990]
libor['LIBOR O/N'] = [float(x) if x != '.' else -999 for x in libor['LIBOR O/N']]

# euribor
# source: https://sdw.ecb.europa.eu/browseTable.do?org.apache.struts.taglib.html.TOKEN=3e266278992a6aed38398e53423a0cb7&df=true&ec=&dc=&oc=&pb=&rc=&DATASET=0&removeItem=&removedItemList=&mergeFilter=&activeTab=EON&showHide=&MAX_DOWNLOAD_SERIES=500&SERIES_MAX_NUM=50&node=9689692&legendRef=reference&legendNor=
e = pd.read_csv('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/Interest Rates/euribor.csv')
e = e.iloc[5:,:]
e['Date'] = [pd.DataFrame(e.index).iloc[x,:][0][0] for x in range(pd.DataFrame(e.index).shape[0])]
e['Date'] = [pd.to_datetime(x) for x in e['Date']]
e['Month'] = [x.month for x in e['Date']]
e['Year'] = [x.year for x in e['Date']]
e['EURIBOR O/N'] = [pd.DataFrame(e.index).iloc[x,:][0][1] for x in range(pd.DataFrame(e.index).shape[0])]
e = e[['Date', 'Year', 'Month', 'EURIBOR O/N']].reset_index(drop = True)
e = e[e['Year'] >= 1990]
elist = []
for y in tqdm(e['Year'].unique()):
    y_df = e[e['Year'] == y]
    for m in y_df['Month'].unique():
        m_df = y_df[y_df['Month'] == m].reset_index(drop = True)
        ym = pd.DataFrame()
        ym.loc[0,'Year'] = y
        ym.loc[0,'Month'] = m
        min_date = min(m_df['Date'])
        ym.loc[0,'EURIBOR O/N'] = m_df.loc[m_df['Date'] == min_date]['EURIBOR O/N'].values[0]
        elist.append(ym)
e = pd.concat(elist, ignore_index = True)
e['EURIBOR O/N'] = [float(x) if x != '.' else -999 for x in e['EURIBOR O/N']]

# FFR
f = pd.read_csv('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/Interest Rates/us_ffr.csv')
f.columns = ['Date', 'US_FFR']
f['Date'] = [pd.to_datetime(x) for x in f['Date']]
f['Month'] = [x.month for x in f['Date']]
f['Year'] = [x.year for x in f['Date']]
f = f[['Year', 'Month', 'US_FFR']]
f = f[f['Year'] >= 1990]

f.shape
libor.shape
e.shape
out = libor.merge(e,
                  how = 'outer',
                  on = ['Year', 'Month'])
out.shape
out = out.merge(f,
                how = 'outer',
                on = ['Year', 'Month'])
out.to_csv('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/Interest Rates/IRs_FFR_LIBOR_EURIBOR_TM_20200708.csv')
