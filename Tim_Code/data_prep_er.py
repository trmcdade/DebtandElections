import pandas as pd
import numpy as np
import os
from os import listdir
import matplotlib.pyplot as plt
from matplotlib import patches as mpatches
from sklearn.preprocessing import StandardScaler
import dateparser
from tqdm import tqdm
from joblib import Parallel, delayed

"""
This piece of code prepares exchange rate data to be included in the main dataset.
"""

dir = "C:/Users/trmcd/Dropbox/Debt Issues and Elections"
# outname = dir + '/Tim_Code/Output_Data/cmcm_scaled_regdata_2020-10-26.csv'
# outname = dir + '/Tim_Code/Output_Data/cmcm_regression_data_2020-10-26.csv'
# df = pd.read_csv(outname, index_col = 0)

ername = dir + '/Explanatory Vars/Exchange rates/imf_xr_1994_2020.csv'
er = pd.read_csv(ername)
er.columns = [x.strip() for x in er.iloc[1,:]]
er = er.iloc[2:,:].reset_index(drop = True)
er = er.melt(id_vars = ['Date'], var_name = 'Curr', value_name = 'LC/SDR')
er['Curr'] = [x.split('(')[1].split(')')[0] for x in er['Curr']]
er = er[pd.notnull(er['Date'])].reset_index(drop = True)
er['Date'] = pd.to_datetime(er['Date'])
er['Month'] = [x.month for x in er['Date']]
er['Year'] = [x.year for x in er['Date']]
er['LC/SDR'] = er['LC/SDR'].astype('float')

# The below currencies are reported infrequently because they had a peg or
# some other reason. So, we forward fill them.
currs = ['PLN', 'PHP', 'PEN', 'MXN', 'MUR', 'EEK', 'CZK', 'CNY', 'CLP']
for curr in tqdm(currs):
    g = er[er['Curr'] == curr].reset_index(drop = True)
    g['LC/SDR'] = g['LC/SDR'].ffill(limit = 5000)

    er = er.merge(g, on = ['Date', 'Year', 'Month', 'Curr'], how = 'left')
    er['LC/SDR_x'] = er['LC/SDR_x'].fillna(er['LC/SDR_y'])
    er.drop(["LC/SDR_y"], inplace = True, axis = 1)
    er.rename(columns = {'LC/SDR_x':'LC/SDR'}, inplace = True)

er['Date'] = er['Date'].astype('str').str[:7]
er['Date'] = [x + '-01' for x in er['Date']]
er = er.groupby(['Date', 'Month', 'Year', 'Curr']).mean().reset_index()

u = er[er['Curr'] == 'USD']

u = u.rename(columns = {'LC/SDR':'USD/SDR'}).drop(['Curr', 'Month', 'Year'], axis = 1).reset_index(drop = True)
er = er.merge(u, on = ['Date'])

er.head()

# unique_combos = er.loc[:,['Curr', 'Date']].drop_duplicates().reset_index(drop = True)
# unique_combos = [(unique_combos.iloc[x,0], unique_combos.iloc[x,1]) for x in range(unique_combos.shape[0])]

er['LC/SDR'] = er['LC/SDR'].astype('float')
er['USD/SDR'] = er['USD/SDR'].astype('float')
er['LC_USD'] = er['LC/SDR'].divide(er['USD/SDR'])
er['LC_USD'] = er['LC_USD'].astype('float')

er = er.reset_index(drop = True)
eroutname = dir + '/Explanatory Vars/Exchange rates/calculated_ers_1994_2020.csv'
er.to_csv(eroutname)
# ers = pd.read_csv(eroutname)
