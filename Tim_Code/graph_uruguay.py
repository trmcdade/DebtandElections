
#%%
import seaborn as sns
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

dir = "C:/Users/trmcd/Dropbox/Debt Issues and Elections/Tim_Code/Output_Data/"
fn = dir + "outstanding_regdata_usd_24m_2021-03-23.csv"
df = pd.read_csv(fn, index_col=0)
df = df[(df['Amt.Issued'] > 0)]
df['Date'] = pd.to_datetime(df['Date'])
# df = df[(df['system'] == 'Presidential') & (df['Amt.Mat'] > 0)]
countries = df['Country'].unique()
[(i, df[df['Country'] == i]['Curr'].unique()) for i in countries]
# df = df[(df['Year'] > 2001) & (df['Year'] < 2003)]

# fig, ax = plt.subplots(figsize=(12, 6))
gdf = df[df['Country'] == 'URUGUAY']
fig = sns.jointplot(data = gdf,
                    x='Date', y='Amt.Issued',
                    # alpha=0.1,
                    hue = 'Curr',
                    xlim = (gdf['Date'].min(), gdf['Date'].max())
                    # ylim = (-0.5, 0.5)
                    )
plt.subplots_adjust(top=0.9)
fig.set_axis_labels('Time', 'Amount of Debt Issued')
# g.fig.suptitle(f'Protest Count Over Time by Country')
plt.tight_layout()
uruguay = dir + f'uruguay_by_curr.png'
plt.savefig(uruguay)

uruguay

# x_dates = df['Date'].dt.strftime('%Y-%m-%d').sort_values().unique()
# ax.set_xticklabels(labels=x_dates, rotation=45, ha='right')

mu = np.mean(df['DV'])
std = np.std(df['DV'])
df['DV2'] = (df['DV'] - mu) / std

# sns.jointplot(data=df, x='Months.To.Election', y='DV', alpha = 0.1, ylim = (0,1))
fig, ax = plt.subplots(figsize=(12, 6))
fig = sns.jointplot(data=df, x='Date', y='Amt.Mat', alpha=0.1)
x_dates = df['Date'].dt.strftime('%Y-%m-%d').sort_values().unique()
ax.set_xticklabels(labels=x_dates, rotation=45, ha='right')

# %%
