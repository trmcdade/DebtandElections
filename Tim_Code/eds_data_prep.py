import pandas as pd
import os
from os import listdir
import difflib
from difflib import ndiff
import numpy as np

# os.getcwd()
# os.chdir('Explanatory Vars')
# os.chdir('IMF EDS')

'''
source: https://databank.worldbank.org/source/quarterly-external-debt-statistics-sdds#
'''

eds = pd.read_csv('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/IMF EBS/eds.csv')
# now: change it from quarterly to monthly.
# years = [x for x in range(1998,2020)]
time = [str(x) for x in eds['Time']]
eds['Year'] = [str(x)[:4] for x in eds['Time']]
eds['Quarter'] = [str(x)[4:6] for x in eds['Time']]

qs = ['Q1', 'Q1', 'Q1', 'Q2', 'Q2', 'Q2', 'Q3', 'Q3', 'Q3', 'Q4', 'Q4', 'Q4']
ms = [str(x) for x in range(1, 13)]
dict = {k:v for k, v in zip(ms, qs)}
combos = pd.DataFrame(list(zip(ms, qs)), columns = ['Month', 'Quarter'])
eds = eds.merge(combos, how = 'right', on = 'Quarter')
eds = eds[(eds['Country Name'] != 'Last Updated: 04/30/2020') & (eds['Country Name'] != 'Data from database: Quarterly External Debt Statistics SDDS') & (eds['Country Name'].notna())]
existing = eds['Country Name'].unique()
# countries = pd.read_excel('../../Tim Code/countries_and_currency_codes.xlsx')['Country'].unique()
countries = pd.read_stata("C:\\Users\\trmcdade\\Dropbox\\Debt Issues and Elections\\working.dta")['country'].unique()
old_entries = [x for x in existing if x not in countries]
new_entries = ['Afghanistan',
                 'Algeria',
                 'Antigua and Barbuda',
                 'Cambodia',
                 'Djibouti',
                 'Dominica',
                 'Euro area',
                 'Guinea-Bissau',
                 'Hong Kong, China',
                 'Kiribati',
                 'Kosovo',
                 'Madagascar',
                 'Nepal',
                 'Macedonia, FYR',
                 'Palau',
                 'Sierra Leone',
                 'Slovakia',
                 'St. Lucia',
                 'Tajikistan',
                 'Tonga',
                 'United States',
                 'West Bank and Gaza',
                 'Yemen, Rep.']

# t['countryname'] = [x.upper() if x.upper() in countries else x for x in t.loc[:,'countryname']]
dict = {k: v for k, v in zip(old_entries, new_entries)}
eds['Country Name'] = [dict[c] if c in old_entries else c for c in eds.loc[:,'Country Name']]
eds.to_csv('IMF_EDS_2020_TM_20200616.csv')
# outpath = '\\Explanatory Vars\\WB EDS\\WB_EDS_2020_TM_20200616.csv'
