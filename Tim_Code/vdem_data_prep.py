import pandas as pd
import os
from os import listdir
import numpy as np
from tqdm import tqdm

v = pd.read_csv('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/VDem/Country_Date_V-Dem_CSV_v10/V-Dem-CD-v10.csv')
vdem_cols = [
             'country_name'
             ,'year'
             ,'historical_date'
             ,'project'
             ,'historical'
             ,'histname'
             ,'v2x_EDcomp_thick' # to what extent is the principle of democracy achieved?
             ,'v2x_api' # to what extent is the electoral principle of democracy achieved?
             ,'v2x_mpi' # to what extent is the electoral principle of democracy achieved?
]

v = v[(v['year'] >= 1990)][vdem_cols]

outlist = []
for ctry in tqdm(v['country_name'].unique()):
    ctry_df = v[v['country_name'] == ctry]
    for y in ctry_df['year'].unique():
        year_df = ctry_df[ctry_df['year'] == y].reset_index(drop = True)
        cy = pd.DataFrame()
        cy.loc[0,'country_name'] = ctry
        cy.loc[0,'year'] = y
        year_df = year_df[['v2x_EDcomp_thick', 'v2x_api', 'v2x_mpi']].dropna(how = 'all')
        if year_df.shape[0] == 0:
            cy.loc[0,'v2x_EDcomp_thick'] = np.nan
            cy.loc[0,'v2x_api'] = np.nan
            cy.loc[0,'v2x_mpi'] = np.nan
        else:
            cy.loc[0,'v2x_EDcomp_thick'] = np.nanmax(year_df['v2x_EDcomp_thick'])
            cy.loc[0,'v2x_api'] = np.nanmax(year_df['v2x_api'])
            cy.loc[0,'v2x_mpi'] = np.nanmax(year_df['v2x_mpi'])
        outlist.append(cy)

out = pd.concat(outlist, ignore_index = True)
out

existing = np.sort(out['country_name'].unique())
# countries = pd.read_excel('../../Tim Code/countries_and_currency_codes.xlsx')['Country'].unique()
countries = sorted(pd.read_stata("C:/Users/trmcdade/Dropbox/Debt Issues and Elections/working.dta")['country'].unique())
old_entries = sorted([x for x in existing if x not in countries])
new_entries = ['Afghanistan',
                 'Algeria',
                 'Bhutan',
                 'Burma/Myanmar',
                 'Burundi',
                 'Cambodia',
                 'Cabo Verde',
                 'Central African Republic',
                 'Chad',
                 'Comoros',
                 'Cuba',
                 'Congo, Dem. Rep.',
                 'Djibouti',
                 'Egypt, Arab Rep.',
                 'Equatorial Guinea',
                 'Eritrea',
                 'Eswatini',
                 'Germany',
                 'Guinea',
                 'Guinea-Bissau',
                 'Guyana',
                 'Haiti',
                 'Hong Kong, China',
                 'Iran',
                 "Cote d'Ivoire",
                 'Kosovo',
                 'Kyrgyz Republic',
                 'Laos',
                 'Liberia',
                 'Libya',
                 'Madagascar',
                 'Malawi',
                 'Maldives',
                 'Mali',
                 'Mauritania',
                 'Nepal',
                 'Niger',
                 'North Korea',
                 'Macedonia, FYR',
                 'Palestine/Gaza',
                 'Palestine/West Bank',
                 'Congo, Rep.',
                 'Russian Federation',
                 'Sao Tome and Principe',
                 'Sierra Leone',
                 'Somalia',
                 'Somaliland',
                 'Korea, Rep.',
                 'South Sudan',
                 'South Yemen',
                 'Sudan',
                 'Syria',
                 'Tajikistan',
                 'Tanzania',
                 'The Gambia',
                 'Timor-Leste',
                 'Togo',
                 'Turkmenistan',
                 'United States of America',
                 'Uzbekistan',
                 'Vanuatu',
                 'Venezuela, RB',
                 'Yemen',
                 'Zanzibar',
                 'Zimbabwe']

# t['countryname'] = [x.upper() if x.upper() in countries else x for x in t.loc[:,'countryname']]
to_be_changed = [x for x in existing if x not in countries]
dict = {k: v for k, v in zip(old_entries, new_entries)}
out['country_name'] = [dict[c] if c in old_entries else c for c in out['country_name']]

out.to_csv('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/VDem/VDem_elections_v10_TM_20200708.csv')
