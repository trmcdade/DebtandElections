import pandas as pd
import numpy as np
import os
from os import listdir
import matplotlib.pyplot as plt
from matplotlib import patches as mpatches
import datetime
import dateparser
from tqdm import tqdm
from joblib import Parallel, delayed
from sklearn.preprocessing import MinMaxScaler


class graphs():

    """
    This is the second piece of code in the project. 
    It prepares the main dataset for use in regression analyses. 
    It prepares datasets at two levels of analysis: 
    1. Country-month-currency 
    2. Country-month-currency-maturity(months)

        self.filter_data()
        self.create_data()
        self.convert_curr()
        self.calculate_pct()
        self.scale_data()
        final_outname = self.dir + f'/Tim_Code/Output_Data/final_regdata_{self.scaled}_{self.pull_date}.csv'
        self.combined.to_csv(final_outname)

    To do so, it:
    1. Takes as input the main dataset
    2. Incorporates other data such as exchange rates, 
    3. Applies some filters
    4. Sums across relevant axes
    5. Standardizes the data so regression coefficients are more interpretable
    6. Constructs plots. 
    """


    def __init__(self, scaled = None):

        self.scaled = scaled
        # self.convert = convert
        self.dir = "C:/Users/trmcd/Dropbox/Debt Issues and Elections/"
        self.pull_date = datetime.datetime.now().strftime('%Y-%m-%d')

        eroutname = self.dir + '/Explanatory Vars/Exchange rates/calculated_ers_1994_2020.csv'
        self.ers = pd.read_csv(eroutname, index_col = 0)

        ## IMPORTANT: 
        # Getting the right input files here is crucial. 
        # cmc is supposed to be the total amount issued at the country month curr level. 
        # cmcm is supposed to be the total amount issued at the country month curr mty level.

        # this should be the intermediate output of get_regression_data_outstanding.py, 
        # which outputs the total amount issued at the country-month-curr level 
        # notwithstanding maturity dates. It's the output before joining in the election data.
        # NO!! Thta's amt outstanding, not amt issued. 

        # cmcname = self.dir + '/Tim_Code/Output_Data/cmcm_regression_data_2020-10-26.csv'
        cmcname = self.dir + '/Tim_Code/Output_Data/share_of_outstanding_2021-03-16.csv'
        self.cmc = pd.read_csv(cmcname, index_col = 0)

        # this is the output of get_regression_data_mat.py, which pulls the share of debt 
        # issued by a given country on a given date with a given maturity length that 
        # matures within n months of the next election. 
        # cmcmname = self.dir + '/Tim_Code/Output_Data/matdata_2020-11-13.csv'
        cmcmname = self.dir + '/Tim_Code/Output_Data/matdata_2021-03-16.csv'
        self.cmcm = pd.read_csv(cmcmname, index_col=0)


    def filter_data(self):

        print('Filtering data. ')
        # cmc is the denominator (total debt issued in month t by country c in curr x). 
        # self.cmc = self.cmc[self.cmc['system'] == 'Presidential'].reset_index(drop = True)
        cmccols = ['Country', 'Date', 'Curr', 'Amt.Issued', 'Amt.Matured']
        self.cmc = self.cmc[cmccols]
        self.cmc = self.cmc.rename(columns = {'Amt.Issued':'Amt.Issued.CMC', 'Amt.Matured':'Amt.Matured.CMC'})
        self.cmc = self.cmc.sort_values(['Country', 'Date', 'Curr']).reset_index(drop = True)
        self.cmc = self.cmc.groupby(['Country', 'Date', 'Curr']).agg(['sum']).reset_index()
        self.cmc.columns = self.cmc.columns.get_level_values(0)

        # cmcm is the numerator (total debt issued in month t by country c in curr x of maturity m).
        # self.cmcm = self.cmcm[self.cmcm['system'] == 'Presidential'].reset_index(drop = True)
        self.cmcm['short_term'] = [1 if x <= 12 else 0 for x in self.cmcm['Mty']]
        # self.cmcm = self.cmcm[pd.notnull(self.cmcm['Next.Election.Date'])]

        # Here, we make sure that we do not omit records for combinations that have no issuances. 
        unique_cmc_combos = self.cmc[['Country', 'Date', 'Curr']].drop_duplicates().sort_values(['Country', 'Date', 'Curr'])
        unique_cmcm_combos = self.cmcm[['Country', 'Date', 'Curr']].drop_duplicates().sort_values(['Country', 'Date', 'Curr'])
        unique_cmc_combos = [(unique_cmc_combos.iloc[x,0], unique_cmc_combos.iloc[x,1], unique_cmc_combos.iloc[x,2]) for x in range(unique_cmc_combos.shape[0])]
        unique_cmcm_combos = [(unique_cmcm_combos.iloc[x,0], unique_cmcm_combos.iloc[x,1], unique_cmcm_combos.iloc[x,2]) for x in range(unique_cmcm_combos.shape[0])]

        # get all the unique combos from each dataset. 
        one = [x for x in unique_cmc_combos if x not in unique_cmcm_combos]
        one = pd.DataFrame(one, columns = ['Country', 'Date', 'Curr'])
        two = [x for x in unique_cmcm_combos if x not in unique_cmc_combos]
        two = pd.DataFrame(two, columns = ['Country', 'Date', 'Curr'])

        # merge them all together so we have all records.
        self.cmc = (self.cmc.merge(one, on=['Country', 'Date', 'Curr'], how='outer', indicator=True).query('_merge != "both"').drop(columns='_merge'))
        self.cmc = (self.cmc.merge(two, on=['Country', 'Date', 'Curr'], how='outer', indicator=True).query('_merge != "both"').drop(columns='_merge'))
        self.cmcm = (self.cmcm.merge(one, on=['Country', 'Date', 'Curr'], how='outer', indicator=True).query('_merge != "both"').drop(columns='_merge'))
        self.cmcm = (self.cmcm.merge(two, on=['Country', 'Date', 'Curr'], how='outer', indicator=True).query('_merge != "both"').drop(columns='_merge'))

        print('Done filtering data.')


    def create_data(self):
        """
        This function divides the sum of bond issuances at the country-month-currency-maturity level
        by the sum of the bond issuances at the country-month-currency level. This pct answers the 
        question: "Of the bonds issued by country C in month t in currency X, what amount had maturity M?
        We divide CMCM by CMC because we need CMC to be the denominator and CMCM to be the numerator. 
        That way we can know how this looks for different maturities.
        """

        print('Joining data.')

        # Get the denominator: 
        # summed issuances at the country-month-currency (cmc) level
        self.cmc = self.cmc.sort_values(['Country', 'Date', 'Curr'], ascending = [True, True, True]).reset_index(drop = True)
        # To do this, we make a string multi-index from the concatenated cmc combinations
        self.cmc['index'] = self.cmc[['Country', 'Date', 'Curr']].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
        self.cmc.set_index('index', inplace = True)
        # Then we drop the index column to leave the summed amount (in local currency). 
        self.cmc2 = self.cmc.drop(['Country', 'Date', 'Curr'], axis = 1)

        # Get the numerator: 
        # summed issuances of a given maturity length m that mature within n months of an election.
        self.cmcm = self.cmcm.sort_values(['Country', 'Date', 'Curr'], ascending = [True, True, True]).reset_index(drop = True)
        self.cmcm['index'] = self.cmcm[['Country', 'Date', 'Curr']].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
        self.cmcm.set_index('index', inplace = True)

        # merge them together on the indices, then drop the indices
        self.combined = self.cmcm.merge(self.cmc2, left_index = True, right_index = True)
        self.combined = self.combined.reset_index(drop = True)

        # merge in exchange rates so we can calculate it all in USD
        self.combined = self.combined.merge(self.ers, on = ['Curr', 'Date'])

        # del self.cmcm
        # del self.cmc
        # del self.cmc2
        self.cmc = self.cmc.reset_index(drop = True)

        print('Done joining data.')


    def convert_curr(self):

        print('Converting to USD. ')

        scale_cols = sorted([x for x in self.combined.columns if 'Amt.' in x])
        for col in tqdm(scale_cols):
            self.combined[col + '_USD'] = self.combined[col].divide(self.combined['LC_USD'])

        self.cmc = self.cmc.merge(self.ers[['Curr', 'Date', 'LC_USD']].drop_duplicates(), on = ['Curr', 'Date'])
        cmc_scale_cols = sorted([x for x in self.cmc.columns if 'Amt.' in x])
        for col in tqdm(cmc_scale_cols):
            self.cmc[col + '_USD'] = self.cmc[col].divide(self.cmc['LC_USD'])

        print('Done converting to USD. ')


    def calculate_pct(self):
        """
        Divide numerator by denominator to get the share of debt xyxyxyxy
        """

        print('Calculating percentage columns.')

        # calc_cols = sorted([x for x in self.combined.columns if ('Pct.' in x and '_USD' not in x)])
        amt_cols = sorted([x for x in self.combined.columns if ('Amt.' in x and '_USD' not in x)])
        calc_col_stems = [x[3:] for x in amt_cols]
        calc_cols = ['Pct' + x for x in calc_col_stems]

        # the divisor should not be the converted amount.

        for i in tqdm(range(len(calc_cols))):
            self.combined[calc_cols[i]] = self.combined[amt_cols[i]].divide(self.combined['Amt.Issued.CMC'])

        print('Done calculating percentage columns.')


    def scale_data(self):

        scaler = MinMaxScaler()
        dpi = pd.read_csv(self.dir + '/Explanatory Vars/WB DPI/WB_DPI_2017_TM_20200615.csv', index_col = 0, low_memory = False)
        dpicols = dpi.loc[:,~dpi.columns.duplicated()].columns

        self.combined = self.combined.drop(['month', 'year'], axis = 1)

        index_cols = ['Country', 'Date', 'Curr', 'Mty']
        dpi_cols = [x for x in self.combined.columns if x in dpicols]
        pct_cols = [x for x in self.combined.columns if 'Pct' in x]
        other_cols = ['tight_election', 'easy_win', 'tight_leader5pc', 'tight_leader', 'easy_leader', 'inv_grade', 'short_term', 'Month', 'Year', 'LC/SDR', 'USD/SDR', 'LC_USD']
        months_cols = [x for x in self.combined.columns if 'Months.' in x]
        amt_cols = [x for x in self.combined.columns if 'Amt.' in x]

        not_scale_list = index_cols + pct_cols + dpi_cols + other_cols + months_cols
        scale_cols = [x for x in self.combined.columns if x not in not_scale_list]
        transformed = pd.DataFrame(scaler.fit_transform(self.combined[scale_cols]), columns = scale_cols)
        self.combined = pd.concat([self.combined[not_scale_list], transformed], axis = 1)
        self.combined = self.combined[index_cols + months_cols + amt_cols + pct_cols + [x for x in scale_cols if x not in amt_cols] + dpi_cols + other_cols]


    def plot_all_countries(self):

        print('Graphing all countries.')
        fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(9, 3), sharey = True, sharex = True)
        colors = ['blue', 'red', 'green']
        legend_labels = ['Mty. > 12m', 'Mty. <= 12m', 'All Debt']
        small = self.combined[['Country', 'Date', 'Months.To.Election', 'Amt.Matured.CMC_USD', 'Amt.Issued.CMC_USD', 'short_term']].drop_duplicates()
        for ctry in sorted(small['Country'].unique()):
            temp = small[small['Country'] == ctry]
            title = 'Amount of Debt Maturing Monthly'
            axes[0].bar('Months.To.Election', 'Amt.Matured.CMC_USD', alpha = 0.1, data = temp[temp['short_term'] == 1], color = colors[0], label = legend_labels[0])
            axes[1].bar('Months.To.Election', 'Amt.Matured.CMC_USD', alpha = 0.1, data = temp[temp['short_term'] == 0], color = colors[1], label = legend_labels[1])
            axes[2].bar('Months.To.Election', 'Amt.Matured.CMC_USD', alpha = 0.1, data = temp, color = colors[2], label = legend_labels[2])
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
        plt.subplots_adjust(top=0.85)
        fig.tight_layout()
        figname = self.dir + 'Tim_Code/graphs/amt_matured_all_countries.png'
        plt.savefig(figname, dpi=300, bbox_inches='tight', format = 'png')
        # fig.savefig(figname, dpi=300, bbox_inches='tight')
        # plt.show()

        print('Done graphing all countries.')


    def plot_one_country(self, ctry):


        print(f'Graphing {ctry}.')
        pct_cols = [x for x in self.combined.columns if 'Pct' in x]
        small = self.combined[['Country', 'Date', 'Months.To.Election', 'Amt.Matured.CMC_USD', 'Amt.Issued.CMC_USD', 'short_term'] + pct_cols].drop_duplicates()
        temp = small[small['Country'] == ctry]
        # [x for x in temp.columns]
        # nlist = ['0', '1', '3', '6', '12']
        colors = ['blue', 'red', 'green']
        legend_labels = ['Mty. > 12m', 'Mty. <= 12m', 'All Debt']
        fig, axes = plt.subplots(nrows=1, ncols=len(legend_labels), figsize=(9, 3), sharey = True, sharex = True)
        # for ctry in sorted(df['Country'].unique()):
        axes[0].bar('Months.To.Election', 'Pct.Issued.Mat.to.Election<12m', alpha = 0.8, data = temp[temp['short_term'] == 1], color = colors[0], label = legend_labels[0])
        axes[1].bar('Months.To.Election', 'Pct.Issued.Mat.to.Election<12m', alpha = 0.8, data = temp[temp['short_term'] == 0], color = colors[1], label = legend_labels[1])
        axes[2].bar('Months.To.Election', 'Pct.Issued.Mat.to.Election<12m', alpha = 0.8, data = temp, color = colors[2], label = legend_labels[2])
        blue_patch = mpatches.Patch(color=colors[0], label=legend_labels[0])
        red_patch = mpatches.Patch(color=colors[1], label=legend_labels[1])
        green_patch = mpatches.Patch(color=colors[2], label=legend_labels[2])
        fig.suptitle(f'Debt Issued by {ctry} Maturing <12 Months Before an Election')
        for ax in axes.flat:
            ax.set(xlabel='Months to Election', ylabel='Pct. Issued Debt')
        handles_labels = [ax.get_legend_handles_labels() for ax in fig.axes]
        handles, labels = [sum(lol, []) for lol in zip(*handles_labels)]
        handles = [x[0][0] for x in handles_labels]
        labels = [x[1][0] for x in handles_labels]
        fig.legend(handles, labels, loc='upper right')
        plt.subplots_adjust(top=0.85)
        fig.tight_layout()
        figname = self.dir + f'Tim_Code/graphs/amt_matured_{ctry}.png'
        plt.savefig(figname, dpi=300, bbox_inches='tight', format = 'png')
        # fig.savefig(figname, dpi=300, bbox_inches='tight')
        # plt.show()
        print(f'Done graphing {ctry}.')



    def main(self):

        self.filter_data()
        self.create_data()
        self.convert_curr()
        self.calculate_pct()
        self.scale_data()
        final_outname = self.dir + f'/Tim_Code/Output_Data/final_regdata_{self.scaled}_{self.pull_date}.csv'
        self.combined.to_csv(final_outname)
        # if self.scaled == 'unscaled':
        #     self.plot_all_countries()
        #     [self.plot_one_country(c) for c in self.combined['Country'].unique()[:4]]
        print('All done.')



if __name__ == "__main__":
    # g = graphs(scaled = 'scaled')
    g = graphs(scaled = 'unscaled')
    g.main()


g.combined['Pct.Issued.Mat.to.Election<12m'].hist()





####
