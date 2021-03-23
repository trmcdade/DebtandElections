import pandas as pd
import os
from os import listdir
import numpy as np
from tqdm import tqdm

cr = pd.read_stata('C:/Users/trmcd/Dropbox/Debt Issues and Elections/Explanatory Vars/Credit Ratings/Domestic & Foreign Credit Ratings.dta')
keep_cols = ['country', 'year', 'month', 'Fitchforeignrating', 'SPforeignrating', 'Moodysforeignrating']
cr = cr[keep_cols]

m = [ 'Aaa', 'Aa1', 'Aa2', 'Aa3', 'A1', 'A2', 'A3',
        'Baa1', 'Baa2', 'Baa3', 'Ba1', 'Ba2', 'Ba3', 'B1', 'B2', 'B3',
        'Caa1', 'Caa2', 'Caa3', 'Ca', 'C', 'WR', '']
m.reverse()
f = ['AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-',
        'BBB+', 'BBB', 'BBB-', 'B+', 'B', 'B-', 'BB+', 'BB', 'BB-',
        'CCC+', 'CCC', 'CCC-', 'CC', 'C',
        'DDD', 'D', 'RD', '']
f.reverse()
sp = ['AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-',
        'BBB+', 'BBB', 'BBB-', 'B+', 'B', 'B-', 'BB+', 'BB', 'BB-',
        'CCC+', 'CCC', 'CCC-', 'CC', 'C',
        'NR', 'SD', '']
sp.reverse()
ig = ['Aaa', 'Aa1', 'Aa2', 'Aa3', 'A1', 'A2', 'A3',
        'Baa1', 'Baa2', 'Baa3',
        'AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-',
        'BBB+', 'BBB', 'BBB-']
ig.reverse()

cr['Fitchforeignrating'] = [f.index(x) for x in cr['Fitchforeignrating']]
cr['SPforeignrating'] = [sp.index(x) for x in cr['SPforeignrating']]
cr['Moodysforeignrating'] = [m.index(x) for x in cr['Moodysforeignrating']]

cr['cr'] = [cr[['Fitchforeignrating', 'SPforeignrating', 'Moodysforeignrating']].iloc[x].max() for x in tqdm(range(cr.shape[0]))]
cr['inv_grade'] = [1 if max(cr.iloc[x,3:].values) > 10 else 0 for x in range(cr.shape[0])]
cr = cr[['country', 'year', 'month', 'cr', 'inv_grade']]
cr.head()

existing = cr['country'].unique()
# countries = pd.read_excel('../../Tim Code/countries_and_currency_codes.xlsx')['Country'].unique()
# countries = pd.read_stata("C:/Users/trmcd/Dropbox/Debt Issues and Elections/working.dta")['country'].unique()
# since we're joining it to the bonds instead, we need to use different country names. 
countries = pd.read_csv("C:/Users/trmcd/Dropbox/Debt Issues and Elections/Tim_Code/Output_Data/Issues/bond_issuances_py.csv", index_col=0)['Country (Full Name)'].unique()
countries
old_entries = [x for x in existing if x.upper() not in countries]
new_entries = ['AZERBAIJAN',
     'CHINA',
     "IVORY COAST",
     'CZECH', 
     'DOMINICAN REPUB.',
     'SOUTH KOREA',
     'Philippines',
     'RUSSIA', 
     'Serbia',
     'Slovak Republic',
     'BRITAIN',
     'VENEZUELA', 
     'Zambia']
# t['countryname'] = [x.upper() if x.upper() in countries else x for x in t.loc[:,'countryname']]
# to_be_changed = [x for x in existing if x not in countries]
dict = {k: v for k, v in zip(old_entries, new_entries)}
cr['country'] = [dict[c] if c in old_entries else c for c in cr.loc[:,'country']]
cr.to_csv('C:/Users/trmcd/Dropbox/Debt Issues and Elections/Explanatory Vars/Credit Ratings/Sov_Credit_Rating.csv')
# out.head()
# outpath = '/Explanatory Vars/WB WDI/WB_macro_2019_TM_20200707.csv'
