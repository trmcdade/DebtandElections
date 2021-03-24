import pandas as pd
dir = "C:/Users/trmcd/Dropbox/Debt Issues and Elections"
# CMC
intermediate_cmc_name = dir + '/Tim_Code/Output_Data/share_of_outstanding_2021-03-17.csv'
final_cmc_name = dir + '/Tim_Code/Output_Data/outstanding_regdata_all_curr_2021-03-17.csv'
cmc = pd.read_csv(intermediate_cmc_name, index_col = 0)

# CMCM
intermediate_cmcm_name = dir + '/Tim_Code/Output_Data/share_of_outstanding_cmcm_2021-03-18.csv'
final_cmcm_name = dir + '/Tim_Code/Output_Data/outstanding_regdata_cmcm_all_curr_2021-03-18.csv'
mat = pd.read_csv(intermediate_cmcm_name, index_col=0)

s_outname = dir + '/Tim_Code/Output_Data/Issues/bond_issuances_py.csv'
s = pd.read_csv(s_outname, index_col=0)

cmc.head()
mat.head()

cmc[(cmc['Country'] == 'ALBANIA') & (cmc['Curr'] == 'ALL') & (cmc['Date'] == '2013-05-01')].reset_index(drop = True)
mat[(mat['Country'] == 'ALBANIA') & (mat['Curr'] == 'ALL')].reset_index(drop=True)['Date'].max()
mat[(mat['Country'] == 'ALBANIA') & (mat['Curr'] == 'ALL') & (mat['Date'] == '2013-05-01')].reset_index(drop=True)
mat[(mat['Country'] == 'ALBANIA') & (mat['Curr'] == 'ALL') & (mat['Date'] == '2013-05-01')].reset_index(drop=True)['Amt.Out'].sum()
s[(s['Country (Full Name)'] == 'ALBANIA') & (s['Curr'] == 'ALL') & (s['Issue Date'].str.startswith('2013-05'))][['Country (Full Name)', 'Curr', 'Issue Date', 'Maturity Date', 'Amount Issued']]
s[(s['Country (Full Name)'] == 'ALBANIA') & (s['Curr'] == 'ALL')][['Country (Full Name)', 'Curr', 'Issue Date', 'Maturity Date', 'Amount Issued']].tail()
# So the Mat code is excluding all dates after 2 Feb 2013. Why?
