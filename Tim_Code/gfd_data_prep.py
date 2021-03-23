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
existing = t['country'].unique()
# countries = pd.read_excel('../../Tim Code/countries_and_currency_codes.xlsx')['Country'].unique()
countries = pd.read_stata("C:\\Users\\trmcdade\\Dropbox\\Debt Issues and Elections\\working.dta")['country'].unique()
old_entries = [x for x in existing if x not in countries]
new_entries = ['Afghanistan' #
               ,'Algeria' #
               ,'Andorra' #
               ,'Antigua and Barbuda' #
               ,'Bermuda' #
               ,'Bhutan' #
               ,'Brunei Darussalam'#
               ,'Burundi' #
               ,'Cambodia'
               ,'Cayman Islands'
               ,'Central African Republic' #
               ,'Chad'
               ,'Channel Islands'
               ,'Comoros' #
               ,'Cuba' #
               ,'Curaçao'
               ,"Cote d'Ivoire"
               ,'Djibouti' #
               ,'Dominica'
               ,'Equatorial Guinea' #
               ,'Eritrea' #
                ,'Eswatini'
               ,'Faroe Islands'
               ,'French Polynesia'
               ,'Gambia, The' #
               ,'Gibraltar'
               ,'Greenland'
               ,'Guam'
               ,'Guinea'
               ,'Guinea-Bissau' #
               ,'Guyana' #
               ,'Haiti' #
               ,'Hong Kong, China'
               ,'Iran, Islamic Rep.' #
               ,'Isle of Man'
               ,'Kiribati' #
               ,"Korea, Dem. People's Rep."
               ,'Kosovo'
               ,'Lao PDR'
               ,'Liberia' #
               ,'Libya' #
               ,'Liechtenstein' #
               ,'Macao SAR, China'
               ,'Madagascar' #
               ,'Malawi' #
               ,'Maldives' #
               ,'Mali' #
               ,'Marshall Islands' #
               ,'Mauritania' #
               ,'Micronesia, Fed. Sts.'
               ,'Monaco'
               ,'Myanmar'
               ,'Nauru'
                ,'Nepal',
                 'New Caledonia',
                 'Niger',
                 'North Macedonia',
                 'Palau',
                 'Puerto Rico',
                 'Samoa',
                 'San Marino',
                 'Sierra Leone',
                 'Sint Maarten (Dutch part)',
                 'Slovak Republic',
                 'Somalia',
                 'South Sudan',
                 'St. Kitts and Nevis',
                 'St. Lucia',
                 'Sudan',
                 'Syrian Arab Republic',
                 'São Tomé and Principe',
                 'Taiwan',
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
                 'Zimbabwe'
                 ]


# t['countryname'] = [x.upper() if x.upper() in countries else x for x in t.loc[:,'countryname']]
to_be_changed = [x for x in existing if x not in countries]
dict = {k: v for k, v in zip(old_entries, new_entries)}

t['country'] = [dict[c] if c in old_entries else c for c in t.loc[:,'country']]

t.to_csv('WB_GFD_2019_TM_20200615.csv')
# outpath = '\\Explanatory Vars\\WB DPI\\WB_GFD_2019_TM_20200615.csv'
