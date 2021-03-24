import pandas as pd
import os
import numpy as np
from os import listdir

t = pd.read_stata('C:/Users/trmcd/Dropbox/Debt Issues and Elections/Explanatory Vars/WB DPI/DPI 2017 (for merge).dta')
t = t[['countryname', 'year', 'maj', 'election_exec', 'election_leg']]

t['year'].sort_values()

for ctry in t['countryname'].unique():
    # print(ctry)
    # if test[(test['countryname'] == ctry) & (test['elect_dum'] == 1) & ((test['elect_exec'] == 1) | (test['elect_leg'] == 1))].shape[0] > 1:
    if t[(t['countryname'] == ctry) & ((t['election_exec'] == 1) | (t['election_leg'] == 1))].shape[0] > 1:
        print('Regular elections in ' + ctry)
    else:
        print('No regular elections in ' + ctry)

existing = t['countryname'].unique()
# countries = pd.read_excel('../../Tim Code/countries_and_currency_codes.xlsx')['Country'].unique()
countries = pd.read_stata("C:/Users/trmcdade/Dropbox/Debt Issues and Elections/working.dta")['country'].unique()

old_entries = [x for x in existing if x not in countries]
new_entries = [
               'Cyprus'
               ,'Afghanistan' #
               ,'United Arab Emirates'
               ,'Burundi' #
               ,'Bahamas, The'
               ,'Bosnia and Herzegovina'
               ,'Brunei'#
               ,'Bhutan'#
               ,'Cent. Af. Rep.' #
               ,'China'
               ,'Congo, Rep.'
               ,'Comoro Is.' #
               ,'Cabo Verde'
               ,'Czech Republic'
               ,'Cuba' #
               ,'GDR' #
               ,'FRG/Germany' #
               ,'Djibouti' #
               ,'Dominican Republic'
               ,'Algeria' #
               ,'Egypt, Arab Rep.'
               ,'Eritrea' #
               ,'United Kingdom'
               ,'Guinea' #
               ,'Gambia' #
               ,'Guinea-Bissau' #
               ,'Eq. Guinea' #
               ,'Guyana' #
               ,'Haiti' #
               ,'Iran' #
               ,'Kyrgyz Republic'
               ,'Cambodia' #
               ,'Korea, Rep.'
               ,'Laos' #
               ,'Liberia' #
               ,'Libya' #
               ,'St. Lucia' #
               ,'Madagascar' #
               ,'Maldives' #
               ,'Macedonia, FYR'
               ,'Mali' #
               ,'Myanmar' #
               ,'Mauritania' #
               ,'Malawi' #
               ,'Niger' #
               ,'Nepal' #
               ,'Papua New Guinea'
               ,'PRK' #
               ,'Russian Federation'
               ,'Sudan'#
               ,'Solomon Islands'
               ,'Sierra Leone' #
               ,'Somalia'#
               ,'South Sudan' #
               ,'Soviet Union' #
               ,'Swaziland' #
               ,'Syria' #
               ,'Chad' #
               ,'Togo' #
               ,'Tajikistan' #
               ,'Turkmenistan' #
               ,'Timor-Leste' #
               ,'Trinidad and Tobago'
               ,'Tanzania' #
               ,'USA' #
               ,'Uzbekistan' #
               ,'Venezuela, RB'
               ,'Vanuatu' #
               ,'Samoa' #
               ,'Yemen' #
               ,'Yemen (PDR)' #
               ,'Yugoslavia' #
                ,'South Africa'
               ,'Congo, Dem. Rep.'
               ,'Zimbabwe' #
]

to_be_changed = [x for x in existing if x not in countries]
dict = {k: v for k, v in zip(old_entries, new_entries)}
t['countryname'] = [dict[c] if c in old_entries else c for c in t['countryname']]
t['year'] = t['year'].dt.strftime('%Y')
t['maj'] = [np.float(x) if isinstance(x, np.float) else np.NAN for x in t['maj']]
reg_elections = pd.DataFrame(t['countryname'].unique())
reg_elections.columns = ['countryname']
maj_threshold = 0.67
min_acceptable_elections = 2
# how many elections between 1990 and 2016 is enough to be regular?
# what is the margin of the majority that we deem 'competitive'?
for ctry in t['countryname'].unique():
    df = t[(t['countryname'] == ctry)]
    if df[(df['maj'] < maj_threshold) & ((df['election_exec'] == 1) | (df['election_leg'] == 1))].shape[0] > min_acceptable_elections:
    # if df[(df['any_election'] == 1) & (df['maj'] < maj_threshold) & ((df['election_exec'] == 1) | (df['election_leg'] == 1))].shape[0] > min_acceptable_elections:
        print('Yes ' + ctry)
        reg_elections.loc[reg_elections['countryname'] == ctry, 'reg_elections'] = 1
    else:
        print('No ' + ctry)
        reg_elections.loc[reg_elections['countryname'] == ctry, 'reg_elections'] = 0

reg_elections.to_csv('Regular_elections_WB_DPI_2017_TM_20200625.csv')
# outpath = '\\Explanatory Vars\\WB DPI\\Regular_elections_WB_DPI_2017_TM_20200625.csv'
