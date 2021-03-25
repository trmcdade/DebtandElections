
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

# check out the currencies in which countries have issued debt.
countries = df['Country'].unique()
[(i, df[df['Country'] == i]['Curr'].unique()) for i in countries]

cmcm_fn = dir + "cmcm_outstanding_regdata_usd_24m_2021-03-23.csv"
df2 = pd.read_csv(cmcm_fn, index_col=0)
df2 = df2[(df2['Amt.Numerator'] > 0)]
df2['Date'] = pd.to_datetime(df2['Date'])

cmcm2_fn = dir + "cmcm_outstanding_regdata_usd_120m_2021-03-23.csv"
df3 = pd.read_csv(cmcm2_fn, index_col=0)
df3 = df3[(df3['Amt.Numerator'] > 0)]
df3['Date'] = pd.to_datetime(df3['Date'])

# cp = sns.color_palette('CMRmap_r')
cp = sns.color_palette('coolwarm')
sns.set_palette('coolwarm', n_colors = len(df3[df3['Country'] == 'URUGUAY']['Curr'].unique()))

## By date of issuance.
gdf = df3[df3['Country'] == 'URUGUAY']
fig = sns.jointplot(data = gdf,
                    x = 'Date', y = 'Amt.Issued',
                    alpha = 0.3,
                    hue = 'Curr',
                    ylim = (0, gdf['Amt.Issued'].max()),
                    xlim = (gdf['Date'].min(), gdf['Date'].max())
                    )
plt.subplots_adjust(top=0.9)
fig.set_axis_labels('Time', 'Amount of Debt Issued')
# g.fig.suptitle(f'')
plt.tight_layout()
uruguay = dir + f'uruguay_curr_time.png'
plt.savefig(uruguay)

## By maturity length.

udf = df3[df3['Country'] == 'URUGUAY']
fig = sns.jointplot(data = udf,
                    x = 'Date', y = 'Mty',
                    alpha = 0.3,
                    hue = 'Curr',
                    xlim = (udf['Date'].min(), udf['Date'].max()),
                    ylim = (-2, udf['Mty'].max() + 1)
                    )
plt.subplots_adjust(top=0.9)
fig.set_axis_labels('Time', 'Maturity (Months)')
# g.fig.suptitle(f'')
plt.tight_layout()
uruguay = dir + f'uruguay_mty_time.png'
plt.savefig(uruguay)

## By maturity length.

mdf = df3[df3['Country'] == 'URUGUAY']
fig = sns.jointplot(data = mdf,
                    x = 'Mty', y = 'Amt.Numerator',
                    alpha = 0.2,
                    hue = 'Curr',
                    xlim = (mdf['Mty'].min() - 1, mdf['Mty'].max() + 1),
                    ylim = (0, mdf['Amt.Numerator'].max())
                    )
plt.subplots_adjust(top=0.9)
fig.set_axis_labels('Maturity (Months)', 'Amount of Debt Issued')
# g.fig.suptitle(f'')
plt.tight_layout()
uruguay = dir + f'uruguay_mty_curr.png'
plt.savefig(uruguay)

mu = np.mean(df['DV'])
std = np.std(df['DV'])
df['DV2'] = (df['DV'] - mu) / std

# sns.jointplot(data=df, x='Months.To.Election', y='DV', alpha = 0.1, ylim = (0,1))
fig, ax = plt.subplots(figsize=(12, 6))
fig = sns.jointplot(data=df, x='Date', y='Amt.Mat', alpha=0.1)
x_dates = df['Date'].dt.strftime('%Y-%m-%d').sort_values().unique()
ax.set_xticklabels(labels=x_dates, rotation=45, ha='right')

# %%
