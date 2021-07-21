import pandas as pd

dir = "C:/Users/trmcd/Dropbox/Debt Issues and Elections"
s = pd.read_csv(dir + '/Tim_Code/Output_Data/Issues/bond_issuances_py.csv', index_col = 0)
s['Year'] = [int(x.split('-')[0]) for x in s['Issue Date']]

len(s['Country (Full Name)'].unique())
len(s['Security Name'].unique())
s[['Issue Date', 'Bloomberg ID', 'Security Name', 'Cpn', 'Maturity Date']].drop_duplicates()

## filter for only the issuances that show up in our final dataset
# read in the final regression data summed at currency level:
outdata = pd.read_csv(dir + "/Tim_Code/Output_Data/outstanding_regdata_usd_24m_2021-04-19.csv", index_col = 0)
# filter for polity and amt maturing >0.
outdata = outdata[outdata['polity2'] >= 5]
outdata = outdata[(outdata['Amt.Mat'] > 0)]
cycombos = outdata[['Country', 'Year']].drop_duplicates().sort_values(['Country', 'Year']).reset_index(drop = True)
final = s.merge(cycombos, how = 'right',
                left_on = ['Country (Full Name)', 'Year'],
                right_on = ['Country', 'Year'])
final[['Country', 'Year', 'Security Name']]

s.shape
s['Issue Year'] = [x.split('-')[0] for x in s['Issue Date']]
len(s[s['Issue Year'] == '1990']['Country (Full Name)'].unique())
len(s[s['Issue Year'] == '1995']['Country (Full Name)'].unique())
len(s[s['Issue Year'] == '2000']['Country (Full Name)'].unique())
len(s[s['Issue Year'] == '2010']['Country (Full Name)'].unique())
len(s[s['Issue Year'] == '2015']['Country (Full Name)'].unique())
s[s['Issue Date'].max()
s['Issue Date'].min()
