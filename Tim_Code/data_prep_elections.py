import pandas as pd
import os
from os import listdir

# os.getcwd()
# os.chdir('Explanatory Vars')
# os.chdir('WB GFD')

# defs = pd.read_excel('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/WB GFD/October2019globalfinancialdevelopmentdatabase.xlsx', 'Definitions and Sources')
# [x for x in defs['Indicator Name'].unique() if 'debt ' in x]
# defs['Indicator Name'].unique()

# cols = ['country'
#         ,'year'
#         ,'gfdddm04' # domestic public debt to gdp
#         ,'gfdddm06' # int'l public debt to gdp
#         # Controls
#         ,'gfddoi19' # banking crisis dummy
#          ,'gfddom01' ## listed companies per 1MM ppl
#          ,'gfddom02' # stock market return pct yoy
#          ,'gfddoe01' # CPI, 2010 = 100, December
#          ,'gfddoe02' # CPI, 2010 = 100, average
#          ,'ny_gdp_mktp_cd' # GDP (Current USD)
#          ,'ny_gdp_pcap_kd' # GDP (Constant 2005 USD)
#          ,'ny_gdp_mktp_cd' # GNP (Current USD)
#          ,'sp_pop_totl' # Total pop
#           ]

# t = pd.read_excel('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/WB GFD/October2019globalfinancialdevelopmentdatabase.xlsx', 'Data - October 2019')
# t = t[cols]
# t.head()

# countries = pd.read_excel('../../Tim Code/countries_and_currency_codes.xlsx')['Country'].unique()
elections = pd.read_stata('C:/Users/trmcd/Dropbox/Debt Issues and Elections/Explanatory Vars/WB DPI/DPI2020/DPI2020.dta')
elections = elections[elections['dateexec'] != -999]
elections['month'] = elections['dateexec']
elections['vote_margin'] = elections['gov1vote'].subtract(elections['gov2vote'])
# elections = pd.read_csv('C:/Users/trmcd/Dropbox/Debt Issues and Elections/Explanatory Vars/WB DPI/DPI2020/WB_DPI_2020_TM_20210419.csv', index_col = 0)
keep_cols = ['countryname', 'year', 'month', 'system', 'vote_margin', 'percentl', 'percent1']
elections = elections[keep_cols].reset_index(drop = True)
elections = elections.rename(columns = {'countryname':'country'})
elections['year'] = elections['year'].astype('str').str[:4]
elections['year'] = elections['year'].astype('int')
elections = elections[pd.notnull(elections['month'])]
elections['month'] = round(elections['month'].astype('int'))
elections['month'] = [str(x) if x > 9 else '0'+str(x) for x in elections['month']]
elections = elections[elections['year'] >= 1990].reset_index(drop = True)
cols = ['year', 'month']
elections['date'] = elections[cols].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)
elections['date'] = elections['date'] + '-01'
elections['month'] = round(elections['month'].astype('int'))

# elections.head()

# create: exec vote share in only round
elections['exec_only_round'] = [elections['percent1'].iloc[x] if elections['percentl'].iloc[x] == -999 else -999 for x in range(elections.shape[0])]
# create: exec vote share in the first round of >1 rounds
elections['exec_first_round'] = [elections['percent1'].iloc[x] if elections['percentl'].iloc[x] != -999 else -999 for x in range(elections.shape[0]) ]
# create: exec vote share in the final round of >1 rounds
elections['exec_final_round'] = [elections['percentl'].iloc[x] if elections['percentl'].iloc[x] != -999 else -999 for x in range(elections.shape[0])]
elections['exec_vote_share'] = [elections['percent1'].iloc[x] for x in range(elections.shape[0])]

elections.head()

# elections = pd.read_stata("C:\\Users\\trmcdade\\Dropbox\\Debt Issues and Elections\\working.dta")
existing = sorted(elections['country'].unique())
s = pd.read_csv("C:/Users/trmcd/Dropbox/Debt Issues and Elections/Tim_Code/bond_issuances_py.csv", index_col = 0)
s = s.dropna(subset=['Country (Full Name)'])
join_names = sorted(s['Country (Full Name)'].unique())
old_entries = [x for x in existing if x.upper() not in join_names]
new_entries = ['AFGHANISTAN',
            'ALGERIA',
            'BOSNIA-HERZE.',
            'CAPE VERDE',
            'CENTRAL AFRICA',
            'COMORO IS.',
            'DEM.REP. CONGO',
            "IVORY COAST",
            'CUBA',
            'DJIBOUTI',
            'DOMINICAN REPB.',
            'EQ. GUINEA',
            'GUINEA',
            'HAITI',
            'IRAN',
            'MALI',
            'MAURITANIA',
            'NIGER',
            'SOUTH KOREA',
            'SAMOA',
            'SOMALIA',
            'RUSSIA',
            'SUDAN',
            'SYRIA',
            'TIMOR-LESTE',
            'CYPRUS',
            'TURKMENISTAN',
            'UNITED STATES',
            'VANUATU',
            'YEMEN',
            'YUGOSLAVIA',
            'Zambia']

# t['countryname'] = [x.upper() if x.upper() in countries else x for x in t.loc[:,'countryname']]
# to_be_changed = old_entries
len(old_entries)
len(new_entries)
dict = {k: v for k, v in zip(old_entries, new_entries)}
dict
elections['country_join_to_bonds'] = [dict[c] if c in old_entries else c.upper() for c in elections.loc[:,'country']]

elections[['country', 'country_join_to_bonds']].sample(15)
elections.head()
elections.to_csv('C:/Users/trmcd/Dropbox/Debt Issues and Elections/Tim_Code/elections_20210420.csv')
# outpath = 'Tim_Code/elections.csv'
