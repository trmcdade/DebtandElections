import pandas as pd
import os
from os import listdir

# p = pd.read_csv('C:/Users/trmcd/Dropbox/Debt Issues and Elections/Explanatory Vars/PolityIV/original/pv_for_debt_2020_10_12.csv', index_col = 0)

p = pd.read_excel('C:/Users/trmcd/Dropbox/Debt Issues and Elections/Explanatory Vars/PolityIV/original/p5v2018.xls')
# theoretically, I'm most interested in government functioning (parcomp)
p_cols = ['country', 'year', 'democ', 'autoc', 'polity2', 'parcomp']
p = p[p_cols]
p = p[p['year'] >= 1990].reset_index(drop = True)
existing = p['country'].unique()
countries = pd.read_stata("C:/Users/trmcd/Dropbox/Debt Issues and Elections/working.dta")['country'].unique()
old_entries = sorted([x for x in existing if x not in countries])
# are all polity countrnames the same as the TE countrynames? if so, below will be empty.
# if not, I need to modify existing so they snap to 'countries'.
# sorted(old_entries)
new_entries = ['Afghanistan',
                 'Algeria',
                 'Bhutan',
                 'Bosnia',
                 'Burundi',
                 'Cambodia',
                 'Cabo Verde',
                 'Central African Republic',
                 'Chad',
                 'Comoros',
                 'Congo, Rep.',
                 'Congo, Dem. Rep.',
                 'Congo, Rep.',
                 "Cote d'Ivoire",
                 'Cuba',
                 'Czech Republic',
                 'Djibouti',
                 'Egypt, Arab Rep.',
                 'Equatorial Guinea',
                 'Eritrea',
                 'Gambia',
                 'Germany',
                 'Germany',
                 'Guinea',
                 'Guinea-Bissau',
                 'Guyana',
                 'Haiti',
                 'Iran',
                 "Cote d'Ivoire",
                 'Korea North',
                 'Korea, Rep.',
                 'Kosovo',
                 'Kyrgyz Republic',
                 'Laos',
                 'Liberia',
                 'Libya',
                 'Macedonia, FYR',
                 'Madagascar',
                 'Malawi',
                 'Mali',
                 'Mauritania',
                 'Myanmar (Burma)',
                 'Nepal',
                 'Niger',
                 'Russian Federation',
                 'Serbia',
                 'Sierra Leone',
                 'Slovakia',
                 'Somalia',
                 'South Sudan',
                 'Sudan',
                 'Sudan-North',
                 'Swaziland',
                 'Syria',
                 'Tajikistan',
                 'Tanzania',
                 'Timor Leste',
                 'Togo',
                 'Turkmenistan',
                 'United Arab Emirates',
                 'USSR',
                 'United States',
                 'Uzbekistan',
                 'Venezuela, RB',
                 'Yemen',
                 'Yemen North',
                 'Yemen South',
                 'Yugoslavia',
                 'Zimbabwe']
dict = {k: v for k, v in zip(old_entries, new_entries)}

p['country'] = [dict[c] if c in old_entries else c for c in p['country']]
# p.sample(20)

# clean it up so there's a month value that we can join on later.
months = [str(x) if x > 9 else str(0)+str(x) for x in range(1, 13)]
years = [x for x in p['year'].unique()]
dflist = list()
for x in years:
    df = pd.DataFrame()
    df['month'] = months
    df['year'] = x
    dflist.append(df)
df = pd.concat(dflist)
p = p.merge(df, on = ['year'])

# t = t[t['Country'].isin(countries)]
# min_date = pd.read_csv(te_filename, index_col = 0)['Date'].min()
# t = t[t['Date'] >= min_date]
p.head()
p.to_csv('C:/Users/trmcd/Dropbox/Debt Issues and Elections/Explanatory Vars/PolityIV/original/pv_for_debt_2020_10_12.csv')
