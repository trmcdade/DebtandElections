import pandas as pd
import os
from os import listdir
import numpy as np

cr = pd.read_stata('C:/Users/trmcd/Dropbox/Debt Issues and Elections/Explanatory Vars/Credit Ratings/Domestic & Foreign Credit Ratings.dta')
keep_cols = ['country', 'year', 'month', 'Fitchforeignrating', 'SPforeignrating', 'Moodysforeignrating']
cr = cr[keep_cols]

m = [ 'Aaa', 'Aa1', 'Aa2', 'Aa3', 'A1', 'A2', 'A3',
        'Baa1', 'Baa2', 'Baa3', 'Ba1', 'Ba2', 'Ba3', 'B1', 'B2', 'B3',
        'Caa1', 'Caa2', 'Caa3', 'Ca', 'C', 'WR', '']
f = ['AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-',
        'BBB+', 'BBB', 'BBB-', 'B+', 'B', 'B-', 'BB+', 'BB', 'BB-',
        'CCC+', 'CCC', 'CCC-', 'CC', 'C',
        'DDD', 'D', 'RD', '']
sp = ['AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-',
        'BBB+', 'BBB', 'BBB-', 'B+', 'B', 'B-', 'BB+', 'BB', 'BB-',
        'CCC+', 'CCC', 'CCC-', 'CC', 'C',
        'NR', 'SD', '']
ig = ['Aaa', 'Aa1', 'Aa2', 'Aa3', 'A1', 'A2', 'A3',
        'Baa1', 'Baa2', 'Baa3',
        'AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-',
        'BBB+', 'BBB', 'BBB-']

cr['Fitchforeignrating'] = [f.index(x) for x in cr['Fitchforeignrating']]
cr['SPforeignrating'] = [sp.index(x) for x in cr['SPforeignrating']]
cr['Moodysforeignrating'] = [m.index(x) for x in cr['Moodysforeignrating']]
cr['inv_grade'] = [1 if min(cr.iloc[x,3:].values) < 10 else 0 for x in range(cr.shape[0])]
cr = cr[['country', 'year', 'month', 'inv_grade']]
cr.head()

existing = cr['country'].unique()
# countries = pd.read_excel('../../Tim Code/countries_and_currency_codes.xlsx')['Country'].unique()
countries = pd.read_stata("C:/Users/trmcd/Dropbox/Debt Issues and Elections/working.dta")['country'].unique()
old_entries = [x for x in existing if x not in countries]
new_entries = ['Azerbaijan',
     'China',
     'Egypt, Arab Rep.',
     'Korea, Rep.',
     'Philippines',
     'Slovakia',
     'Venezuela, RB']
# t['countryname'] = [x.upper() if x.upper() in countries else x for x in t.loc[:,'countryname']]
# to_be_changed = [x for x in existing if x not in countries]
dict = {k: v for k, v in zip(old_entries, new_entries)}
cr['country'] = [dict[c] if c in old_entries else c for c in cr.loc[:,'country']]
cr.to_csv('C:/Users/trmcd/Dropbox/Debt Issues and Elections/Explanatory Vars/Credit Ratings/Sov_Credit_Rating.csv')
# out.head()
# outpath = '/Explanatory Vars/WB WDI/WB_macro_2019_TM_20200707.csv'
