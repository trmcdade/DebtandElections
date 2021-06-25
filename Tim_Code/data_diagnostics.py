import pandas as pd

dir = "C:/Users/trmcd/Dropbox/Debt Issues and Elections"
s = pd.read_csv(dir + '/Tim_Code/Output_Data/Issues/bond_issuances_py.csv', index_col = 0)

s.head()

len(s['Country (Full Name)'].unique())
len(s['Security Name'].unique())
s[['Issue Date', 'Bloomberg ID', 'Security Name', 'Cpn', 'Maturity Date']].drop_duplicates()


s.shape
s['Issue Year'] = [x.split('-')[0] for x in s['Issue Date']]
len(s[s['Issue Year'] == '1990']['Country (Full Name)'].unique())
len(s[s['Issue Year'] == '1995']['Country (Full Name)'].unique())
len(s[s['Issue Year'] == '2000']['Country (Full Name)'].unique())
len(s[s['Issue Year'] == '2010']['Country (Full Name)'].unique())
len(s[s['Issue Year'] == '2015']['Country (Full Name)'].unique())
s[s['Issue Date'].max()
s['Issue Date'].min()
