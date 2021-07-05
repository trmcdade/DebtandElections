import pandas as pd
import os
from os import listdir

c = pd.read_stata('C:/Users/trmcd/Dropbox/coming_to_terms_data.dta')
out = pd.read_csv('C:/Users/trmcd/Dropbox/Debt Issues and Elections/Tim_Code/Output_Data/cmcm_outstanding_regdata_usd_24m_2021-04-19.csv', index_col = 0)
ctry_list = [x for x in out['Country'].unique()]

ccode_dict = pd.read_excel('C:/Users/trmcd/Dropbox/ccode_dict.xlsx').sort_values('ccode')
ccode_dict['Country'] = [x.upper() for x in ccode_dict['Country']]
ccode_dict = {ccode_dict['ccode'].iloc[x]: ccode_dict['Country'].iloc[x] for x in range(ccode_dict.shape[0])}
ccode_dict['BOL'] = 'BOLIVIA'
ccode_dict['PHL'] = 'PHILIPPINES'
[x for x in ctry_list if x not in ccode_dict.values() ]
ccode_dict = {k:v for (k,v) in ccode_dict.items() if ccode_dict[k] in ctry_list}

# ccode_dict
# filter to only the countries that are in the output dataset.
c = c[c['ccode'].isin(ccode_dict.keys())].reset_index(drop = True)
keep_cols = ['year', 'month', 'ccode', 'crisis_currency', 'crisis_inflation', 'crisis_sovdebt']
c = c[keep_cols]
c['Country'] = [ccode_dict[c['ccode'].iloc[x]] for x in range(c.shape[0])]

c.to_csv('C:/Users/trmcd/Dropbox/Debt Issues and Elections/Explanatory Vars/Crises/crises_20210705.csv')
