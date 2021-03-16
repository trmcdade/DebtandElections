import pandas as pd
import os
from os import listdir
import numpy as np

inf = pd.read_csv('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/WB WDI/inflation_data.csv')
yr_cols = [f'{x} [YR{x}]' for x in range(1970, 2020)]
inf_def = inf[inf['Series Name'] == 'Inflation, GDP deflator (annual %)']
inf_def = pd.melt(inf_def,
                  id_vars=['Country Name', 'Country Code'],
                  value_vars = yr_cols,
                  var_name='Year',
                  value_name='Inflation (GDP Deflator, annual %)')
inf_def['Year'] = [pd.to_datetime(x[:4]).year for x in inf_def['Year']]

inf_cpi = inf[inf['Series Name'] == 'Inflation, consumer prices (annual %)']
inf_cpi = pd.melt(inf_cpi,
                  id_vars=['Country Name', 'Country Code'],
                  value_vars = yr_cols,
                  var_name='Year',
                  value_name='Inflation (Consumer Prices, annual %)')
inf_cpi['Year'] = [pd.to_datetime(x[:4]).year for x in inf_cpi['Year']]

gdppc = pd.read_csv('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/WB WDI/gdppc_data.csv')
gdppc = gdppc[gdppc['Series Name'] == 'GDP per capita growth (annual %)']
gdppc = pd.melt(gdppc,
                id_vars=['Country Name', 'Country Code'],
                value_vars = yr_cols,
                var_name='Year',
                value_name='GDPPC growth')
gdppc['Year'] = [pd.to_datetime(x[:4]).year for x in gdppc['Year']]

out = pd.merge(inf_def,
               inf_cpi,
               # how = 'inner',
               left_on = ['Country Name', 'Country Code', 'Year'],
               right_on = ['Country Name', 'Country Code', 'Year'])
out = pd.merge(out,
               gdppc,
               left_on = ['Country Name', 'Country Code', 'Year'],
               right_on = ['Country Name', 'Country Code', 'Year'])

out = out.replace('..', np.NaN)

# t = pd.read_excel('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/WB GFD/October2019globalfinancialdevelopmentdatabase.xlsx', 'Data - October 2019')
existing = out['Country Name'].unique()
# countries = pd.read_excel('../../Tim Code/countries_and_currency_codes.xlsx')['Country'].unique()
countries = pd.read_stata("C:/Users/trmcdade/Dropbox/Debt Issues and Elections/working.dta")['country'].unique()
old_entries = [x for x in existing if x not in countries]
new_entries = ['Afghanistan',
             'Algeria',
             'American Samoa',
             'Andorra',
             'Antigua and Barbuda',
             'Bermuda',
             'Bhutan',
             'British Virgin Islands',
             'Brunei Darussalam',
             'Burundi',
             'Cambodia',
             'Cayman Islands',
             'Central African Republic',
             'Chad',
             'Channel Islands',
             'Comoros',
             'Cuba',
             'Curacao',
             'Djibouti',
             'Dominica',
             'Equatorial Guinea',
             'Eritrea',
             'Eswatini',
             'Faroe Islands',
             'French Polynesia',
             'Gambia, The',
             'Gibraltar',
             'Greenland',
             'Guam',
             'Guinea',
             'Guinea-Bissau',
             'Guyana',
             'Haiti',
             'Hong Kong SAR, China',
             'Iran, Islamic Rep.',
             'Isle of Man',
             'Kiribati',
             'Korea, Dem. People’s Rep.',
             'Kosovo',
             'Lao PDR',
             'Liberia',
             'Libya',
             'Liechtenstein',
             'Macao SAR, China',
             'Madagascar',
             'Malawi',
             'Maldives',
             'Mali',
             'Marshall Islands',
             'Mauritania',
             'Micronesia, Fed. Sts.',
             'Monaco',
             'Myanmar',
             'Nauru',
             'Nepal',
             'New Caledonia',
             'Niger',
             'Macedonia, FYR',
             'Northern Mariana Islands',
             'Palau',
             'Puerto Rico',
             'Samoa',
             'San Marino',
             'Sao Tome and Principe',
             'Sierra Leone',
             'Sint Maarten (Dutch part)',
             'Slovakia',
             'Somalia',
             'South Sudan',
             'St. Kitts and Nevis',
             'St. Lucia',
             'St. Martin (French part)',
             'Sudan',
             'Syrian Arab Republic',
             'Tajikistan',
             'Tanzania',
             'Timor-Leste',
             'Togo',
             'Tonga',
             'Turkmenistan',
             'Turks and Caicos Islands',
             'Tuvalu',
             'United States',
             'Uzbekistan',
             'Vanuatu',
             'Virgin Islands (U.S.)',
             'West Bank and Gaza',
             'Yemen, Rep.',
             'Zimbabwe',
             'Arab World',
             'Caribbean small states',
             'Central Europe and the Baltics',
             'Early-demographic dividend',
             'East Asia & Pacific',
             'East Asia & Pacific (excluding high income)',
             'East Asia & Pacific (IDA & IBRD countries)',
             'Euro area',
             'Europe & Central Asia',
             'Europe & Central Asia (excluding high income)',
             'Europe & Central Asia (IDA & IBRD countries)',
             'European Union',
             'Fragile and conflict affected situations',
             'Heavily indebted poor countries (HIPC)',
             'High income',
             'IBRD only',
             'IDA & IBRD total',
             'IDA blend',
             'IDA only',
             'IDA total',
             'Late-demographic dividend',
             'Latin America & Caribbean',
             'Latin America & Caribbean (excluding high income)',
             'Latin America & the Caribbean (IDA & IBRD countries)',
             'Least developed countries: UN classification',
             'Low & middle income',
             'Low income',
             'Lower middle income',
             'Middle East & North Africa',
             'Middle East & North Africa (excluding high income)',
             'Middle East & North Africa (IDA & IBRD countries)',
             'Middle income',
             'North America',
             'Not classified',
             'OECD members',
             'Other small states',
             'Pacific island small states',
             'Post-demographic dividend',
             'Pre-demographic dividend',
             'Small states',
             'South Asia',
             'South Asia (IDA & IBRD)',
             'Sub-Saharan Africa',
             'Sub-Saharan Africa (excluding high income)',
             'Sub-Saharan Africa (IDA & IBRD countries)',
             'Upper middle income',
             'World']

# t['countryname'] = [x.upper() if x.upper() in countries else x for x in t.loc[:,'countryname']]
to_be_changed = [x for x in existing if x not in countries]
dict = {k: v for k, v in zip(old_entries, new_entries)}

out['Country Name'] = [dict[c] if c in old_entries else c for c in out.loc[:,'Country Name']]
out[out['Year'] >= 1990].head()

out.to_csv('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/WB WDI/WB_macro_2019_TM_20200707.csv')
# out.head()
# outpath = '/Explanatory Vars/WB WDI/WB_macro_2019_TM_20200707.csv'
