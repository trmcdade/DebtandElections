import pandas as pd
import numpy as np
import os
from os import listdir
import matplotlib.pyplot as plt
from matplotlib import patches as mpatches
from sklearn.preprocessing import StandardScaler
import dateparser
from tqdm import tqdm
from joblib import Parallel, delayed

dir = "C:/Users/trmcd/Dropbox/Debt Issues and Elections"
# b = pd.read_csv(dir + '/Tim_Code/Output_Data/bond_issuances_py.csv', index_col = 0)
# b[b['Maturity (Months)'] < 0][['Country (Full Name)', 'Issue Date', 'Maturity Date', 'Maturity (Months)']]

# outname = dir + '/Tim_Code/OldOutputData/Round9/regression_data_2020-10-26.csv'
# outname = dir + '/Tim_Code/Output_Data/cmcm_scaled_regdata_2020-10-26.csv'
outname = dir + '/Tim_Code/Output_Data/cmcm_regression_data_2020-10-26.csv'
df = pd.read_csv(outname, index_col = 0)
df['short_term'] = [1 if x <= 12 else 0 for x in df['Mty']]

eroutname = dir + '/Explanatory Vars/Exchange rates/calculated_ers_1994_2020.csv'
ers = pd.read_csv(eroutname, index_col = 0)
ers['Date'] = [x + '-01' for x in ers['Date']]
all = df.merge(ers, on = ['Curr', 'Date'])
scale_cols = sorted([x for x in df.columns if 'Amt.' in x])
for col in tqdm(scale_cols):
    all[col + '_USD'] = all[col] / all['LC_USD']
df = all

def plot_all_countries(what = 'amt_matured'):
    if what not in ['amt_matured', 'amt_share']:
        print('Error! Pick amt_matured or amt_share.')
    fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(9, 3), sharey = True, sharex = True)
    colors = ['blue', 'red', 'green']
    legend_labels = ['Mty. > 12m', 'Mty. < 12m', 'All Debt']
    for ctry in sorted(df['Country'].unique()):
        print(ctry)
        temp = df[df['Country'] == ctry]
        if what == 'amt_matured':
            title = 'Amount of Debt Maturing Monthly'
            axes[0].bar('Months.To.Election', 'Amt.Matured', alpha = 0.1, data = temp[temp['short_term'] == 1], color = colors[0], label = legend_labels[0])
            axes[1].bar('Months.To.Election', 'Amt.Matured', alpha = 0.1, data = temp[temp['short_term'] == 0], color = colors[1], label = legend_labels[1])
            axes[2].bar('Months.To.Election', 'Amt.Matured', alpha = 0.1, data = temp, color = colors[2], label = legend_labels[2])
        elif what == 'amt_share':
            title = 'Amount of Debt Issued Monthly Maturing <6 Months Before an Election'
            axes[0].bar('Months.To.Election', 'Amt.Issued.Mat.to.Election<6m', alpha = 0.1, data = temp[temp['short_term'] == 1], color = colors[0], label = legend_labels[0])
            axes[1].bar('Months.To.Election', 'Amt.Issued.Mat.to.Election<6m', alpha = 0.1, data = temp[temp['short_term'] == 0], color = colors[1], label = legend_labels[1])
            axes[2].bar('Months.To.Election', 'Amt.Issued.Mat.to.Election<6m', alpha = 0.1, data = temp, color = colors[2], label = legend_labels[2])
    blue_patch = mpatches.Patch(color=colors[0], label=legend_labels[0])
    red_patch = mpatches.Patch(color=colors[1], label=legend_labels[1])
    green_patch = mpatches.Patch(color=colors[2], label=legend_labels[2])
    fig.suptitle(title)
    for ax in axes.flat:
        ax.set(xlabel='Months to Election', ylabel='Amount of Debt')
    handles_labels = [ax.get_legend_handles_labels() for ax in fig.axes]
    handles, labels = [sum(lol, []) for lol in zip(*handles_labels)]
    handles = [x[0][0] for x in handles_labels]
    labels = [x[1][0] for x in handles_labels]
    fig.legend(handles, labels, loc='upper right')
    fig.tight_layout()
    figname = dir + f'/Tim_Code/graphs/{what}.png'
    fig.savefig(figname)
    plt.show()

plot_all_countries(what = 'amt_matured')


plot_all_countries(what = 'amt_share')





outname1 = dir + '/Tim_Code/Output_Data/cmc_regression_data_2020-10-26.csv'
df1 = pd.read_csv(outname1, index_col = 0)


def plot_one_country(ctry):

    temp = df[df['Country'] == ctry]
    # [x for x in temp.columns]
    # nlist = ['0', '1', '3', '6', '12']
    colors = ['blue', 'red', 'green']
    legend_labels = ['Mty. > 12m', 'Mty. < 12m', 'All Debt']
    fig, axes = plt.subplots(nrows=1, ncols=len(legend_labels), figsize=(9, 3), sharey = True, sharex = True)
    # for ctry in sorted(df['Country'].unique()):
    axes[0].bar('Months.To.Election', 'Pct.Issued.Mat.to.Election<12m', alpha = 0.8, data = temp[temp['short_term'] == 1], color = colors[0], label = legend_labels[0])
    axes[1].bar('Months.To.Election', 'Pct.Issued.Mat.to.Election<12m', alpha = 0.8, data = temp[temp['short_term'] == 0], color = colors[1], label = legend_labels[1])
    axes[2].bar('Months.To.Election', 'Pct.Issued.Mat.to.Election<12m', alpha = 0.8, data = temp, color = colors[2], label = legend_labels[2])
    blue_patch = mpatches.Patch(color=colors[0], label=legend_labels[0])
    red_patch = mpatches.Patch(color=colors[1], label=legend_labels[1])
    green_patch = mpatches.Patch(color=colors[2], label=legend_labels[2])
    fig.suptitle(f'Pct of Debt Issued by {ctry} Maturing <12 Months Before an Election')
    for ax in axes.flat:
        ax.set(xlabel='Months to Election', ylabel='Amount of Debt Issued')
    handles_labels = [ax.get_legend_handles_labels() for ax in fig.axes]
    handles, labels = [sum(lol, []) for lol in zip(*handles_labels)]
    handles = [x[0][0] for x in handles_labels]
    labels = [x[1][0] for x in handles_labels]
    fig.legend(handles, labels, loc='upper right')
    fig.tight_layout()
    # figname = dir + '/Tim_Code/graphs/time_to_any_election.png'
    # fig.savefig(figname)
    plt.show()


df[(df['Country'] == 'PERU')]['Pct.Issued.Mat.to.Election<12m']

[plot_one_country(c) for c in df['Country'].unique()[:4]]




# within one country I want to show that the amount of debt issued over time
# that matures before an election decreases as the election gets closer.
# therefore I should have two: one for short-term debt and one for long-term debt.
