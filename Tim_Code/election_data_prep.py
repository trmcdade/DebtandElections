import pandas as pd
import os
from os import listdir

# os.getcwd()
# os.chdir('Explanatory Vars')
# os.chdir('WB GFD')

defs = pd.read_excel('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/WB GFD/October2019globalfinancialdevelopmentdatabase.xlsx', 'Definitions and Sources')
[x for x in defs['Indicator Name'].unique() if 'debt ' in x]
# defs['Indicator Name'].unique()

cols = ['country'
        ,'year'
        ,'gfdddm04' # domestic public debt to gdp
        ,'gfdddm06' # int'l public debt to gdp
        # Controls
        ,'gfddoi19' # banking crisis dummy
         ,'gfddom01' ## listed companies per 1MM ppl
         ,'gfddom02' # stock market return pct yoy
         ,'gfddoe01' # CPI, 2010 = 100, December
         ,'gfddoe02' # CPI, 2010 = 100, average
         ,'ny_gdp_mktp_cd' # GDP (Current USD)
         ,'ny_gdp_pcap_kd' # GDP (Constant 2005 USD)
         ,'ny_gdp_mktp_cd' # GNP (Current USD)
         ,'sp_pop_totl' # Total pop
          ]

t = pd.read_excel('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/WB GFD/October2019globalfinancialdevelopmentdatabase.xlsx', 'Data - October 2019')
t = t[cols]
t.head()
# countries = pd.read_excel('../../Tim Code/countries_and_currency_codes.xlsx')['Country'].unique()
elections = pd.read_stata("C:\\Users\\trmcdade\\Dropbox\\Debt Issues and Elections\\working.dta")
existing = elections['country'].unique()
s = pd.read_excel("C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Tim_Code/Bloomberg Sovereign Bond Data_updated2019_TM.xlsx")
s = s.dropna(subset=['Country (Full Name)'])
join_names = sorted(s['Country (Full Name)'].unique())
old_entries = [x for x in countries if x.upper() not in join_names]
new_entries = ['UAE',
            'BAHAMAS',
            'BOSNIA-HERZE.',
            "IVORY COAST",
            'CONGO',
            'CAPE VERDE',
            'CZECH',
            'DOMINICAN REPB.',
            'EGYPT',
            'BRITAIN',
            'HONG KONG',
            'KYRGYZSTAN',
            'SOUTH KOREA',
            'MACEDONIA',
            'Montenegro',
            'PAPUA N.GUINEA',
            'RUSSIA',
            'SOLOMON ISLAND',
            'TRINIDAD AND TO',
            'ST. VINCENT',
            'VENEZUELA',
            'SERBIA',
            'DEM.REP. CONGO',
            'Zambia']

# t['countryname'] = [x.upper() if x.upper() in countries else x for x in t.loc[:,'countryname']]
to_be_changed = old_entries
dict = {k: v for k, v in zip(old_entries, new_entries)}
dict
elections['country_join_to_bonds'] = [dict[c] if c in to_be_changed else c.upper() for c in elections.loc[:,'country']]
elections[['country', 'country_join_to_bonds']].sample(15)
elections.to_csv('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Tim_Code/elections.csv')
# outpath = 'Tim_Code/elections.csv'
