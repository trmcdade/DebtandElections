import pandas as pd
import os
from os import listdir
#
# os.getcwd()
# os.chdir('Explanatory Vars')
# os.chdir('WB DPI')

t = pd.read_stata('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/WB DPI/DPI 2017 (for merge).dta')

dpi_cols = ['countryname', 'wbcode'
            ,'year', 'month'
            ,'system' # parliamentary (2), assembly-elected president (1), presidential (0)
            # years in office for CE, finite term (1/0), years left in current term
            ,'yrsoffc', 'finittrm', 'yrcurnt', 'multpl'
            # president and defense minister are military?
            ,'military', 'defmin'
            # president pct of votes in 1st/only round or last round
            ,'percent1', 'percentl'
            # party of CE has been in office for how long
            ,'prtyin'
            ,'execme' #name of party
            # policy orientation: econ, nationalist, rural, regional, religious
            ,'execrlc', 'execnat', 'execrurl'
            ,'execreg', 'execrel', 'execage'
            # party of exec control all relative houses?
            ,'allhouse'
            # party affiliation of the non-chief exec (e.g. president not PM)
            ,'nonchief'
            # legislature:
            ,'totalseats'
            # sum of squared seat shares of all parties in government/opposition
            ,'herfgov', 'herfopp', 'herftot'
            ,'gov1me', 'gov1seat', 'gov1vote', 'gov1rlc', 'gov1nat', 'gov1rurl', 'gov1reg', 'gov1rel', 'gov1age'
            ,'gov2me', 'gov2seat', 'gov2vote', 'gov2rlc', 'gov2nat', 'gov2rurl', 'gov2reg', 'gov2rel', 'gov2age'
            ,'gov3me', 'gov3seat', 'gov3vote', 'gov3rlc', 'gov3nat', 'gov3rurl', 'gov3reg', 'gov3rel', 'gov3age'
            ,'govoth', 'govothst', 'govothvt'
            ,'opp1me', 'opp1seat', 'opp1vote', 'opp1rlc', 'opp1nat', 'opp1rurl', 'opp1reg', 'opp1rel', 'opp1age'
            ,'opp2me', 'opp2seat', 'opp2vote'
            ,'opp3me', 'opp3seat', 'opp3vote'
            ,'oppoth', 'oppothst', 'oppothvt'
            ,'ulprty' # number of unaligned parties
            ,'numul' # number of seats for unaligned parties
            ,'ulvote' # vote share for unaligned parties
            ,'oppmajh' # does one opp party have an abs majority in the House?
            ,'oppmajs' # does one opp party have an abs majority in the Senate?
            ,'mdmh' #mean district magnitude house
            ,'mdms' #mean district magnitude senate
            ,'ssh' #number of senate seats divided by total congressional seats
            ,'pluralty' #winner-take all yes or no
            ,'pr' # proportional representation
            ,'housesys' # PR or plurality in House elections?
            ,'sensys' # PR or plurality in Senate elections?
            ,'thresh' # threshold for representation
            ,'dhondt' # Is the D'Hondt system used?
            ,'cl' # Are closed lists used?
            ,'select' # Candidate selection: 1 for National, 2 for sub-national, 3 for primary
            ,'fraud' # serious fraud or intimidation?
            ,'auton' # are there autonomous regions?
            ,'muni' # are municipal gov'ts locally elected?
            ,'state' # are state governments locally elected?
            ,'author' # do states have tax/spending/legislative authority?
            ,'stconst' # are the senators' constituencies the states?
            ,'numgov', 'numopp' # number of seats
            ,'numvote', 'oppvote' # vote shares
            ,'maj' # margin of majority
            ,'partyage' # average age of parties
            ,'dateleg' # month when parliamentary elections were held
            ,'dateexec' # month when presidential elections were held
            ,'legelec' # was there a leg election in this year
            ,'exelec' # was there a exec election in this year
            ,'liec' # leg index of electoral competitiveness. 1 low, 7 high.
            ,'eiec' # exec index of electoral competitiveness. 1 low, 7 high.
            ,'herfgov', 'herfopp', 'herftot' #HHI of the government by party
            ,'frac', 'oppfrac', 'govfrac'
            ,'tensys_strict', 'tensys' # How long has teh country been autocratic or democratic?
            ,'checks_lax', 'checks' # checks and balances yes or no
            ,'stabs_strict', 'stabs' # pct of veto players who drop from the gov't in a given year
            ,'stabns_strict', 'stabns' # same as above but ignores the second chamber
            ,'tenlong_strict', 'tenlong' # longest tenure of a veto player
            ,'tenshort_strict', 'tenshort' # shortest tenure of a veto player
            ,'polariz' # maximum polarization b/x exec party and four principal parties of legis
            ]


# dpi_cols = ['countryname', 'year','month'
#             # parliamentary (2), assembly-elected president (1), presidential (0)
#             ,'system'
#             # years in office for CE, finite term (1/0), years left in current term
#             ,'yrsoffc', 'finittrm'#, 'yrscurnt'
#             # president pct of votes in 1st/only round or last round
#             ,'percent1', 'percentl'
#             # party of CE has been in office for how long
#             # ,'partyin'
#             # policy orientation: econ, nationalist, rural, regional, religious
#             ,'execrlc', 'execnat', 'execrurl'
#             ,'execreg', 'execrel'
#             # party of exec control all relative houses?
#             ,'allhouse'
#             # party affiliation of the non-chief exec (e.g. president not PM)
#             ,'nonchief'
#             # legislature:
#             # sum of squared seat shares of all parties in government/opposition
#             ,'herfgov', 'herfopp'
#             # month when presidential/parliamentary elections were held
#             # ,'dateeleg'
#             ,'dateexec'
#             # margin of majority
#             ,'maj'
#             # 1 if there was an election of this type in this year
#             ,'legelec', 'exelec']

existing = t['countryname'].unique()
# countries = pd.read_excel('../../Tim Code/countries_and_currency_codes.xlsx')['Country'].unique()
countries = pd.read_stata("C:\\Users\\trmcdade\\Dropbox\\Debt Issues and Elections\\working.dta")['country'].unique()

t = t[dpi_cols]
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

# t['countryname'] = [x.upper() if x.upper() in countries else x for x in t.loc[:,'countryname']]
to_be_changed = [x for x in existing if x not in countries]
dict = {k: v for k, v in zip(old_entries, new_entries)}
t['countryname'] = [dict[c] if c in old_entries else c for c in t['countryname']]

t['year'] = t['year'].dt.strftime('%Y')
t['year']
type(t.loc[0,'month'])

t.to_csv('C:/Users/trmcdade/Dropbox/Debt Issues and Elections/Explanatory Vars/WB DPI/WB_DPI_2017_TM_20200615.csv')
# outpath = '\\Explanatory Vars\\WB DPI\\WB_DPI_2017_TM_20200615.csv'
# [x for x in xlist if x not in t.columns]
# xlist = ['oppmajs', 'ssh', 'military', 'tenshort_strict', 'dateleg', 'execme', 'fraud', 'cl', 'frac', 'oppfrac', 'oppmajh', 'stabs_strict', 'tenshort', 'herftot', 'tenlong', 'govfrac', 'prtyin', 'execage', 'stabns_strict', 'numopp', 'yrcurnt', 'totalseats', 'housesys', 'ulprty', 'tensys', 'ulvote', 'muni', 'defmin', 'tensys_strict', 'stabns', 'author', 'dhondt', 'numgov', 'Days.Since.Most.Recent.Election<180d', 'stconst', 'checks', 'state', 'numvote', 'mdms', 'stabs', 'polariz', 'select', 'checks_lax', 'sensys', 'tenlong_strict', 'oppvote', 'pluralty', 'mdmh', 'pr', 'thresh', 'numul', 'multpl', 'auton', 'liec', 'Days.Since.Most.Recent.Election<90d', 'partyage']
old_entries = [
    'Turk Cyprus',
    'Bosnia-Herz',
    'Cent. Af. Rep.',
    'PRC',
    "Cote d'Ivoire",
    'Comoro Is.',
    'C. Verde Is.',
    'Czech Rep.',
    # 'GDR',
    # 'FRG/Germany',
    'Dom. Rep.',
    'UK',
    'Eq. Guinea',
    'Iran',
    'ROK',
    'St. Lucia',
    'Mali',
    'Niger',
    'P. N. Guinea',
    'PRK',
    'Sudan',
    'Solomon Is.',
    # 'Soviet Union'
    'Syria',
    'Trinidad-Tobago',
    'Taiwan',
    'USA',
    #'Yemen (PDR)',
    #'Yugoslavia',
    'S. Africa',
    'Congo (DRC)',
    'Zambia']
new_entries = [
    'CYPRUS'
    ,'BOSNIA-HERZE.'
    ,'CENTRAL AFRICA'
    ,'CHINA'
    ,'IVORY COAST'
    ,'COMOROS (THE)'
    ,'CAPE VERDE'
    ,'CZECH'
    # ,'GERMANY'
    # ,'GERMANY'
    ,'DOMINICAN REPUB.'
    ,'BRITAIN'
    ,'EQUATORIAL'
    ,'IRAN (ISLAMIC REPUBLIC OF)'
    ,'SOUTH KOREA'
    ,'SAINT LUCIA'
    ,'MALI REPUBLIC'
    ,'NIGER REPUBLIC'
    ,'PAPUA N. GUINEA'
    ,"KOREA (THE DEMOCRATIC PEOPLE'S REPUBLIC OF)"
    ,'SUDAN (THE)'
    ,'SOLOMON ISLAND'
    # ,'RUSSIA'
    ,'SYRIAN ARAB REPUBLIC'
    ,'TRINIDAD AND TO'
    ,'TAIWAN (PROVINCE OF CHINA)'
    ,'UNITED STATES'
    # ,'YEMEN'
    # ,'SERBIA'
    ,'SOUTH AFRICA'
    ,'DEM.REP. CONGO'
    ,'Zambia'
    ]
