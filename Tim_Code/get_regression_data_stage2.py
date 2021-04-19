import pandas as pd
import numpy as np
import os
from os import listdir
import datetime
from datetime import datetime, timedelta, date
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import dateparser
from tqdm import tqdm
from joblib import Parallel, delayed
import functools
from sklearn.preprocessing import MinMaxScaler


class Debt_Issues:


    def __init__(self, pull_date = None, sum_across_currencies = None, max_mty = None):
        '''
        Tim McDade
        19 April 2021
        Debt Issues and Elections

        This code produces a dataset incorporating issuance-level bond data,
        election data, variables on political systems, and characteristics of
        countries' debt profiles.

        NOTE: THIS IS AT THE LEVEL OF THE MATURITY. EACH COUNTRY ISSUES DEBT IN
        A CURRENCY OF A MATURITY ON A DATE. THE DV IS THAT AMOUNT DIVIDED BY THE
        TOTAL AMOUNT ISSUED ON THAT DATE, AND THE INDEP VAR IS THE TIME FROM
        THAT MATURITY TO THE ELECTION.

        This version calculates the percent of debt issued that matures within
        xx months of the next election.

        General outline:
        0. Initialize and import bond issue data
        1. Import election data: load_election_data
        2. Create the fields we want from the bond issue data: sb_join
        2a. Calculate amount of debt maturing today: sb_join
        2b. Calculate amount of debt outstanding today: sb_join
        3. Convert to USD, if desired: sum_data_across_currencies
        4. Filter the final output, join it with external data on elections etc., and export it.
        '''

        print('Initialized.')
        # dir = "C:/Users/trmcd/Dropbox/Debt Issues and Elections"
        self.dir = "C:/Users/trmcd/Dropbox/Debt Issues and Elections"
        # If running on a Linux VM:
        # self.dir = '/home/trmcdade/DebtandElections'
        self.cores = int(os.cpu_count())
        self.curr_list = pd.read_excel(self.dir + "/Tim_Code/countries_and_currency_codes.xlsx")
        self.curr_list_hist = pd.read_excel(self.dir + "/Tim_Code/countries_and_currency_codes_historical.xlsx")
        self.currencies = pd.concat([self.curr_list_hist, self.curr_list], axis = 0, sort = True)

        if pull_date == None:
            self.pull_date = datetime.now().strftime('%Y-%m-%d')
        else:
            self.pull_date = pull_date

        # Load the bond issuance data.

        s_outname = self.dir + '/Tim_Code/Output_Data/Issues/bond_issuances_py.csv'
        self.s = pd.read_csv(s_outname, index_col = 0)

        # filter to only debt of length less than xyz months.
        if max_mty in [x for x in range(121)]:
            self.s = self.s[self.s['Maturity (Months)'] <= max_mty]
            self.max_mty = max_mty
        elif max_mty == None:
            self.max_mty = 'all'
            self.s = self.s
        else:
            print("Error: Decide what the maximum maturity (in months) of bonds will be.")

        # Here, we're interested in the next election after MATURTIY
        self.s = self.s[['Country (Full Name)', 'Issue Date', 'Maturity (Months)', 'Maturity Date', 'Curr', 'Next Election After Maturity', 'Months.Mat.To.Election', 'Amount Issued']].copy()
        self.s = self.s.rename(columns={'Maturity (Months)': 'Mty'})

        self.s['Issue Date'] = self.s['Issue Date'].str[:7]
        self.s['Maturity Date'] = self.s['Maturity Date'].str[:7]
        # De-dupe here. It's possible that there are have two issuances in the same currency in the same month, which will be reflected in the aggregated sum.
        self.s = self.s.drop_duplicates()
        # Sum the amt issued in each combination and clean up results.
        self.s = self.s.groupby(['Country (Full Name)', 'Issue Date', 'Mty', 'Maturity Date', 'Curr', 'Next Election After Maturity', 'Months.Mat.To.Election']).agg(['sum'], dropna=False).reset_index()
        self.s.columns = self.s.columns.get_level_values(0)

        # sum everything into USD?
        if sum_across_currencies in ['usd','lc']:
            self.sum_across_currencies = sum_across_currencies
        else:
            print("Error: Decide if you're going to sum across currencies.")

        print('Bond data loaded.')


    def load_election_data(self):
        '''
        Load the data about each elections that happened in each country.
        '''

        print('Loading Election Data.')
        ename = self.dir + "/Tim_Code/elections.csv"
        # get the election data from the aggrergated country-month file.
        # self.elections = pd.read_stata(self.dir + "/working.dta")
        self.elections = pd.read_csv(ename, index_col = 0, low_memory = False)
        cols = [
                'year', 'month'
                ,'country', 'country_join_to_bonds'
                ,'countryname'
                ,'date'
                ,"vote_margin"
                ]

        self.e = pd.DataFrame(self.elections[cols])

        print('Election Data Loaded.')


    def sum_data_across_currencies(self, df):

        """
        We sum the amounts across currencies if the below function calls for it.
        The way we do this is by converting to USD and summing USD.
        """

        print('Summing across currencies.')

        eroutname = self.dir + '/Explanatory Vars/Exchange rates/calculated_ers_1994_2020.csv'
        ers = pd.read_csv(eroutname, index_col = 0)
        ers['Date'] = ers['Date'].astype(str).str[:7]

        # Note: we lose some records when we add exchange rates.
        df = df.merge(ers, on = ['Curr', 'Date', 'Month', 'Year'])

        # grab the columns that need to be scaled.
        scale_cols = sorted([x for x in df.columns if 'Amt.' in x])
        # scale them to USD and put the results in new cols.
        for col in scale_cols:
            # LC divided by LC/USD is the same as LC * USD/LC. The LCs cancel and you get USD.
            df[col + '_USD'] = df[col].divide(df['LC_USD'])

        # create DV in converted USD and format year and month cols
        df['DV_USD'] = df['Amt.Numerator_USD'].divide(df['Amt.Denominator_USD'], fill_value = 0)

        print('Done summing across currencies.')

        return df


    def sb_join(self):

        """
        This function calculates the percentage of debt outstanding on a given day that matures on that day.
        The level of analysis is the country-currency level.
        """
        print('Calculating Share Data.')

        existing_file_list = [x for x in os.listdir(self.dir + '/Tim_Code/Output_Data') if x.startswith('cmcm_share_of_outstanding_') and self.pull_date in x and f'{self.max_mty}m_' in x and self.sum_across_currencies in x]
        if len(existing_file_list) > 0:
            # if the file exists already, use it.
            in_name = self.dir + '/Tim_Code/Output_Data/' + existing_file_list[-1]
            self.out = pd.read_csv(in_name, index_col = 0)
        else:
            # create the file from scratch.
            si = self.s.copy()

            # dir = "C:/Users/trmcd/Dropbox/Debt Issues and Elections"
            # s_outname = dir + '/Tim_Code/Output_Data/Issues/bond_issuances_py.csv'
            # s = pd.read_csv(s_outname, index_col = 0)
            # s = s[s['Maturity (Months)'] <= 24]
            #
            # # Here, we're interested in the next election after MATURTIY
            # s = s[['Country (Full Name)', 'Issue Date', 'Maturity (Months)', 'Maturity Date', 'Curr', 'Next Election After Maturity', 'Months.To.Election', 'Amount Issued']].copy()
            # s = s.rename(columns={'Maturity (Months)': 'Mty'})
            # s['Issue Date'] = s['Issue Date'].str[:7]
            # s['Maturity Date'] = s['Maturity Date'].str[:7]
            # s = s.drop_duplicates()
            # s = s.groupby(['Country (Full Name)', 'Issue Date', 'Mty', 'Maturity Date', 'Curr', 'Next Election After Maturity', 'Months.To.Election']).agg(['sum'], dropna=False).reset_index()
            # s.columns = s.columns.get_level_values(0)

            # s.drop(['Mty', 'Maturity Date', 'Next Election After Maturity', 'Months.To.Election'], axis = 1)
            denom_df = si.drop(['Mty', 'Maturity Date', 'Next Election After Maturity', 'Months.Mat.To.Election'], axis = 1).groupby(['Country (Full Name)', 'Issue Date', 'Curr']).agg(['sum'], dropna = False).reset_index()
            denom_df.columns = denom_df.columns.get_level_values(0)

            # write denominator: all debt issued before and maturing after the date in question.
            def output_denominator(ctry, curr, date):
                """
                This function gets the total amount of debt issued in a
                given month for a country-currency combo.
                This will serve as the denominator.
                """
                denominator = denom_df[(denom_df['Country (Full Name)'] == ctry) & (denom_df['Curr'] == curr) & (denom_df['Issue Date'] <= date)]['Amount Issued'].sum()
                # denominator = si[(si['Country (Full Name)'] == ctry) & (si['Curr'] == curr) & (si['Mty'] == mty) & (si['Issue Date'] <= date) & (si['Maturity Date'] >= date)]['Amount Issued'].sum()
                return denominator

            denominator_list = Parallel(n_jobs=int(os.cpu_count()))(delayed(output_denominator)(si['Country (Full Name)'].iloc[i], si['Curr'].iloc[i], si['Issue Date'].iloc[i]) for i in tqdm(range(si.shape[0])))
            # si['Amount Outstanding'] = denominator_list
            si['Amt.Denominator'] = denominator_list

            # ctry = 'ALBANIA'
            # mty = 24
            # curr = 'ALL'
            # date = '2011-05'
            # si = s
            # # how much debt was issued today with mty 24?
            # d = denom_df[(denom_df['Country (Full Name)'] == ctry) & (denom_df['Curr'] == curr) & (denom_df['Issue Date'] == date)]['Amount Issued'].sum()

            # write numerator: all debt outstanding today that matures today
            def output_numerator(ctry, curr, mty, date):
                """
                This function gets the total amount of outstanding debt maturing now; in other words,
                debt issued by a country in a currency that matures today.
                This will serve as the numerator.
                """
                numerator = si[(si['Country (Full Name)'] == ctry) & (si['Curr'] == curr) & (si['Mty'] == mty) & (si['Issue Date'] == date)]['Amount Issued'].sum()
                return numerator

            numerator_list = Parallel(n_jobs=int(os.cpu_count()))(delayed(output_numerator)(si['Country (Full Name)'].iloc[i], si['Curr'].iloc[i], si['Mty'].iloc[i], si['Issue Date'].iloc[i]) for i in tqdm(range(si.shape[0])))
            # si['Amount Maturing'] = numerator_list
            si['Amt.Numerator'] = numerator_list

            # create the DV: amt debt maturing today divided by the amt of debt outstanding today.
            # si['DV'] = si['Amount Maturing'].divide(si['Amount Outstanding'], fill_value = 0)
            si['DV'] = si['Amt.Numerator'].divide(si['Amt.Denominator'], fill_value = 0)

            # clean up the output df.
            # si = si.rename(columns={'Country (Full Name)': 'Country', 'Issue Date': 'Date', 'Next Election After Maturity':'Next.Election', 'Amount Issued': 'Amt.Issued', 'Amount Outstanding': 'Amt.Out', 'Amount Maturing': 'Amt.Mat'})
            # self.out = si[['Country', 'Date', 'Curr', 'Mty', 'Next.Election', 'Months.Mat.To.Election', 'Amt.Issued', 'Amt.Out', 'Amt.Mat', 'DV']]

            si = si.rename(columns={'Country (Full Name)': 'Country', 'Issue Date': 'Date', 'Next Election After Maturity':'Next.Election', 'Amount Issued': 'Amt.Issued'})
            self.out = si[['Country', 'Date', 'Curr', 'Mty', 'Next.Election', 'Months.Mat.To.Election', 'Amt.Issued', 'Amt.Numerator', 'Amt.Denominator', 'DV']]

            self.out['Year'] = self.out['Date'].str[:4].astype('float64')
            self.out['Month'] = self.out['Date'].str[5:].astype('float64')
            # self.out['Date'] = [x + '-01' for x in self.out['Date']]
            self.out = self.out.reset_index(drop = True)

            if self.sum_across_currencies == 'usd':
                self.out = self.sum_data_across_currencies(self.out)

            sboutname = self.dir + f'/Tim_Code/Output_Data/cmcm_share_of_outstanding_{self.sum_across_currencies}_{self.max_mty}m_{self.pull_date}.csv'
            self.out.to_csv(sboutname)

        print('Share Data Calculated.')


    def filter_data(self):
        '''
        filter the data.
        join in the dpi and regular elections stuff with self.e here.
        Then, join in self.e with the regression data here,
        filter out the columns you want.

        Filters:
        quasi-competitive Elections (TBD)
        regular government changes (TBD)
        issued before 1990 (TBD)
        domestic or foreign currency (dealt with)

        '''

        print('Joining all the data together and filtering.')

        # System-level variables come from the WB DPI dataset, managed in dpi_data_prep.py
        # filter for presidential here.
        dpi = pd.read_csv(self.dir + '/Explanatory Vars/WB DPI/DPI2020/WB_DPI_2020_TM_20210419.csv', index_col = 0, low_memory = False)
        dpi = dpi.loc[:,~dpi.columns.duplicated()]
        dpi = dpi[['countryname', 'year', 'system']] # 'month',
        dpi = dpi[dpi['system'] == 'Presidential']

        print('Upon loading, elections dataset has ', len(self.e['country_join_to_bonds'].unique()), 'countries. The date range is ', self.e['year'].min(), 'until ', self.e['year'].max(), '.')

        self.e = self.e.merge(dpi,
                              left_on = ['country', 'year'],#, 'month'],
                              right_on = ['countryname', 'year'])#, 'month'])
        self.e = self.e.sort_values(['country', 'year']) # , 'month'

        # print(f'After merging DPI, elections dataset has {len(self.e['country_join_to_bonds'].unique())} countries. The date range is {self.e['year'].min()} until {self.e['year'].max()}.')
        print('After merging DPI, elections dataset has ', len(self.e['country_join_to_bonds'].unique()), 'countries. The date range is ', self.e['year'].min(), 'until ', self.e['year'].max(),'.')

        # # Debt statistics variables come from the IMF EDS dataset, managed in eds_data_prep.py
        # eds = pd.read_csv(self.dir + '/Explanatory Vars/IMF EDS/IMF_EDS_2020_TM_20200616.csv', index_col = 0)
        # self.e = self.e.merge(eds,
        #                  left_on = ['country', 'year', 'month'],
        #                  right_on = ['Country Name', 'Year', 'Month'])
        #
        # print('After merging EDS, elections dataset has ', len(self.e['country_join_to_bonds'].unique()), 'countries. The date range is ', self.e['year'].min(), 'until ', self.e['year'].max(),'.')

        # filter by only those countries that have regular competitive elections.
        # definitions in reg_elections_data_prep.py
        reg_elections = pd.read_csv(self.dir + '/Explanatory Vars/WB DPI/Regular_elections_WB_DPI_2017_TM_20200625.csv', index_col = 0)
        self.e = self.e.merge(reg_elections,
                                how = 'left',
                              left_on = ['country'],
                              right_on = ['countryname'])

        print('After merging reg_elections, elections dataset has ', len(self.e['country_join_to_bonds'].unique()), 'countries. The date range is ', self.e['year'].min(), 'until ', self.e['year'].max(),'.')

        self.e = self.e[self.e['reg_elections'] == 1].reset_index(drop = True)

        print('After filtering for reg_elections, elections dataset has ', len(self.e['country_join_to_bonds'].unique()), 'countries. The date range is ', self.e['year'].min(), 'until ', self.e['year'].max(),'.')

        # TODO: If you want to use any of these variables, be sure to go into the respective code creating that file
        # and format the country names in the same way (capitalized, mostly) that the bond data country names are formatted.
        # see the credit rating code for an example.

        # polity = pd.read_csv(self.dir + '/Explanatory Vars/PolityIV/original/pv_for_debt_2020_10_12.csv', index_col = 0)
        # self.out = self.out.merge(polity,
        #                       left_on = ['Country', 'Year', 'Month'],
        #                       right_on = ['country', 'year', 'month'])
        #
        # print('After merging polity, out has ', len(self.out['Country'].unique()), 'countries. The date range is ', self.out['Year'].min(), 'until ', self.out['Year'].max(),'.')

        # macro = pd.read_csv(self.dir + '/Explanatory Vars/WB WDI/WB_macro_2019_TM_20200707.csv', index_col = 0)
        # self.out = self.out.merge(macro,
        #                       left_on = ['Country', 'Year'],
        #                       right_on = ['Country Name', 'Year'])
        #
        # print('After merging macro, out has ', len(self.out['Country'].unique()), 'countries. The date range is ', self.out['Year'].min(), 'until ', self.out['Year'].max(),'.')

        # spx = pd.read_csv(self.dir + '/Explanatory Vars/SPX/SPX_historical_2020_TM_20200708.csv', index_col = 0)
        # self.out = self.out.merge(spx,
        #                       left_on = ['Year', 'Month'],
        #                       right_on = ['Year', 'Month'])
        #
        # print('After merging spx, out has ', len(self.out['Country'].unique()), 'countries. The date range is ', self.out['Year'].min(), 'until ', self.out['Year'].max(),'.')

        # vdem = pd.read_csv(self.dir + '/Explanatory Vars/VDem/VDem_elections_v10_TM_20200708.csv', index_col = 0)
        # self.out = self.out.merge(vdem,
        #                         how = 'left',
        #                       left_on = ['Country', 'Year'],
        #                       right_on = ['country_name', 'year'])

        # print('After merging vdem, out has ', len(self.out['Country'].unique()), 'countries. The date range is ', self.out['Year'].min(), 'until ', self.out['Year'].max(),'.')

        # add in interest rates.
        irs = pd.read_csv(self.dir + '/Explanatory Vars/Interest Rates/IRs_FFR_LIBOR_EURIBOR_TM_20200708.csv', index_col = 0)
        # EURIBOR and LIBOR have more missing data than the US Fed Funds Rate.
        irs = irs[['Year', 'Month', 'US_FFR']]
        self.out = self.out.merge(irs,
                                how = 'left',
                              left_on = ['Year', 'Month'],
                              right_on = ['Year', 'Month'])

        print('After merging irs, out has ', len(self.out['Country'].unique()), 'countries. The date range is ', self.out['Year'].min(), 'until ', self.out['Year'].max(),'.')

        # add in sov credit rating and a dummy for whether it's investment grade or not.
        cr = pd.read_csv(self.dir + '/Explanatory Vars/Credit ratings/Sov_Credit_Rating.csv', index_col = 0)
        self.out = self.out.merge(cr,
                                how = 'left',
                              left_on = ['Country', 'Year', 'Month'],
                              right_on = ['country', 'year', 'month'])

        print('After merging credit ratings, out has ', len(self.out['Country'].unique()), 'countries. The date range is ', self.out['Year'].min(), 'until ', self.out['Year'].max(), '.')

        print('out head')
        print(self.out.head())
        print('out cols')
        print([x for x in self.out.columns])
        print('Before merging with elections, out has ', len(self.out['Country'].unique()), 'countries. The date range is ', self.out['Year'].min(), 'until ', self.out['Year'].max(),'.')
        print('The countries in out but not elections are ')
        print([x for x in self.out['Country'].unique() if x not in self.e['country_join_to_bonds'].unique()])
        print('The countries in elections but not out are ')
        print([x for x in self.e['country_join_to_bonds'].unique() if x not in self.out['Country'].unique()])

        self.out = self.out.merge(self.e,
                                  # how = 'outer',
                                  how = 'left',
                                #   left_on = ['Year', 'Month', 'Country'],
                                #   right_on = ['year', 'month', 'country_join_to_bonds'])
                                  left_on = ['Next.Election', 'Country'],
                                  right_on = ['date', 'country_join_to_bonds'])

        print('After merging with elections, out has ', len(self.out['Country'].unique()), 'countries. The date range is ', self.out['Year'].min(), 'until ', self.out['Year'].max(),'.')

        self.out = self.out[pd.notnull(self.out['vote_margin'])]

        print('After filtering out null vote_margins, out has ', len(self.out['Country'].unique()), 'countries. The date range is ', self.out['Year'].min(), 'until ', self.out['Year'].max(),'.')
        print('After filtering out null vote_margins, out has ', self.out.shape, ' records.')

        # order the columns how we want them and get rid of duplicates:
        final_cols = ['Country'
                        ,'Date', 'date'
                        ,'Year', 'year'
                        ,'Month', 'month'
                        ,'Curr'
                        ,'Mty'
                        ,'Amt.Issued', 'Amt.Issued_USD'

                        # ,'Amt.Mat', 'Amt.Out'
                        # ,'Amt.Mat_USD', 'Amt.Out_USD'

                        ,'Amt.Numerator', 'Amt.Denominator'
                        ,'Amt.Numerator_USD', 'Amt.Denominator_USD'

                        ,'LC/SDR', 'USD/SDR', 'LC_USD'
                        ,'DV', 'DV_USD'
                        # ,'Curr_Type'

                        ,'Next.Election'
                        ,'Months.Mat.To.Election'
                        ,'vote_margin'
                        ,'inv_grade'
                        ,'cr'
                        ,'US_FFR'
                       ]

        # filter to the cols we want
        self.out = self.out[[x for x in self.out.columns if x in final_cols]]

        # write it out
        outname = self.dir + f'/Tim_Code/Output_Data/cmcm_outstanding_regdata_{self.sum_across_currencies}_{self.max_mty}m_{self.pull_date}.csv'
        self.out.to_csv(outname)

        print('Regression data written out.')


    def main(self):
        '''
        This runs everything.
        '''
        self.load_election_data()
        self.sb_join()
        self.filter_data()
        print('All done.')



if __name__ == "__main__":
    debt = Debt_Issues(pull_date = datetime.now().strftime('%Y-%m-%d'),
                        sum_across_currencies = 'usd', #'lc'
                        max_mty = 24 # in months)
                        )
    debt.main()

    debt = Debt_Issues(pull_date=datetime.now().strftime('%Y-%m-%d'),
                       sum_across_currencies='usd',  # 'lc'
                       max_mty = 60 # in months)
                       )
    debt.main()

    debt = Debt_Issues(pull_date = datetime.now().strftime('%Y-%m-%d'),
                        sum_across_currencies = 'usd', #'lc'
                        max_mty = 120 # in months)
                        # max_mty = all # in months)
                        )
    debt.main()



###
