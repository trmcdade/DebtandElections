import pandas as pd
import numpy as np
import os
from os import listdir
import datetime
from datetime import datetime
from datetime import timedelta
from datetime import date
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import dateparser
from tqdm import tqdm
from joblib import Parallel, delayed
import functools
from functools import reduce
import time
from sklearn.preprocessing import MinMaxScaler
import math
#
# dir = "C:/Users/trmcd/Dropbox/Debt Issues and Elections"
# # s = pd.read_excel(dir + "/Tim_Code/Bloomberg Sovereign Bond Data_updated2019_TM.xlsx")
# s_outname = dir + '/Tim_Code/Output_Data/bond_issuances_py.csv'
# s = s.dropna(subset=['Country (Full Name)'])
# # TODO: should we exclude all NA Issue dates for bonds issued before 1990?
# # get the data for each country.
# outlist = Parallel(n_jobs=int(os.cpu_count()))(delayed(self.build_country_issue_data)(ctry) for ctry in tqdm(self.s['Country (Full Name)'].unique()))
# s = [pd.read_csv(dir + '/Tim_Code/Output_Data/' + x, index_col = 0) for x in os.listdir(dir + '/Tim_Code/Output_Data/') if 'cmc_issue' in x]
# s = pd.concat(s, ignore_index = True)
# s.head()
# # i want a maturity length bucket
# # self.s = self.s[self.s['Maturity (Yrs)'] < 10]
# s = s[~pd.isnull(s['Maturity (Yrs)'])].reset_index(drop = True)
# s = s[~pd.isnull(s['Maturity Date'])].reset_index(drop = True)
# s['Maturity Date'] = [s['Maturity Date'].iloc[x] if s['Maturity (Yrs)'].iloc[x] > 0 else '20' + s['Maturity Date'].iloc[x][2:] for x in range(s.shape[0])]
#
# def monthsdiff(enddate, startdate):
#     enddate = dateparser.parse(enddate)
#     startdate = dateparser.parse(startdate)
#     return (enddate.year - startdate.year) * 12 + (enddate.month - startdate.month)
#
# s['Maturity (Months)'] = Parallel(n_jobs=int(os.cpu_count()))(delayed(monthsdiff)(enddate = s['Maturity Date'].iloc[row], startdate = s['Issue Date'].iloc[row]) for row in tqdm(range(s.shape[0])))
#
# # TODO: make closest election overall, regardless of before/after.
# # s_outname = self.dir + '/Tim_Code/bond_issuances_py.csv'
# s.to_csv(s_outname)


class Debt_Issues:


    def __init__(self, pull_date = None):
        '''
        Tim McDade
        23 March 2021
        Debt Issues and Elections

        THIS IS THE BIG LONG SLOW ONE, IT MAY OUTPUT SOMETHING USEFUL BUT IT'S BEEN MODIFIED. 
        FIGURE OUT HOW IT RELATES TO THE OTHERS. 

        This code produces a dataset incorporating issuance-level bond data,
        election data, variables on political systems, and characteristics of
        countries' debt profiles.

        This version adds percent of debt issued that matures within xx months of
        the next election.

        General outline:
        1. Import election data: load_election_data
        2. Import bond issue data: load_issue_data
        3. Import currency data: get_currencies
        4. Once this is imported, for each country (get_all_countries_data -->
        get_country_data_all)
        a. Find each country-date-currency combination
        b. For each country-date, find the next election dates (list_it)
        c. For each country-date-curr, find the amt issued, matured,
        fractions, etc. (get_country_vars, which is made up of
        get_curr_per_country_date and get_date_vars for each date in the country).
        5. Concatenate all the country data together.
        6. Filter the final output and export it.
        '''

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


    def load_election_data(self):
        '''
        Load the data about the elections.
        '''

        ename = self.dir + "/Tim_Code/elections.csv"
        # get the election data from the aggrergated country-month file.
        # self.elections = pd.read_stata(self.dir + "/working.dta")
        self.elections = pd.read_csv(ename, index_col = 0, low_memory = False)
        cols = [
                # general variables
                'year', 'month', 'wbcode', 'country', 'country_join_to_bonds'
                ,'countryname','cname', 'exname', 'iso2code', 'regioncode'
                # Independent variables:
                # Election within xx months
                ,"election_exec", "election_last3mo"
                ,"election_last6mo", "election_lastyr", "election_leg"
                ,"election_next3mo", "election_next4_6mo", "election_next6mo"
                ,"election_now", "election_recent"
                ,'cyear', 'scode', 'date', 'date_dmy'
                ,'emonth', 'eyear', 'eday', 'eprec'
                ,'bday', 'bmonth', 'byear', 'bprec', 'post'
                ,'executivename', 'executiveparty'
                #,'interim'
                # Exogeneous/endogenous elections
                # Expected election closeness, using actual election outcome as
                # a proxy, vary for robustness
                ,'executiveelectedby','executivevoteshare'
                ,"easy_5pc", "easy_elect5pc"
                ,"easy_election"
                ,"easy_last3mo", "easy_last6mo", "easy_leader"
                ,"easy_leader5pc", "easy_next3mo", "easy_next6mo"
                ,"easy_win"
                ,"tight_elect5pc","tight_election", "tight_last3mo"
                ,"tight_last6mo", "tight_leader","tight_leader5pc"
                ,"tight_next3mo", "tight_next6mo", "vote_margin"
                ,'any_election','leader_elect'
                ,"eday","EFF","EFF_5mo","eiec","elect"
                ,"elect_dum", "elect_exec", "elect_exec_count"
                ,"elect_leg", "electcomp", "electcomp2"
                ,"elected"
                # Amount of outstanding debt already in a certain tranche.
                # Pct debt less than 1 year on country-year basis
                #     Get average TTM and this data externally since we only
                #     have 1990 after data.
                #     Sources: Global Development Finance dataset,
                #     Global Debt Monitor at Institute for International Finance,
                #     External Debt Statistics hub at IMF quarterly?
                # Elections that are expected to generate a partisan shift could
                # also have different maturity dynamics.
                # Time between election and government formation
                # Profile of bond terms including currency, yield, perhaps even legal terms.
                ,'monthsSinceLastIssue', 'maturity_avg_UT', 'maturity_sd_UT'
                # Macro econ variables: stock market, spreads, credit ratings, etc.
                # Political system variables
                #     Exogeneous/endogeneous elections
                #     Government system
                #     Presidential or parliamentary? Would parliamentary show
                #     up in shorter-time frame bonds?
                ]


        self.e = pd.DataFrame(self.elections[cols])
        # e = pd.DataFrame(elections[cols])

        e_clean_outname = self.dir + "/Tim_Code/elections_clean.csv"
        if len([f for f in listdir(self.dir + '/Tim_Code/') if f.endswith('elections_clean.csv')]) > 0:
            self.e_years = pd.read_csv(e_clean_outname, index_col = 0)
            self.e_years['date'] = pd.to_datetime(self.e_years['date'])
        else:
            # TODO: is this the right filter criteria?
            self.e_years = self.elections.loc[(self.elections['leader_elect'] == 1), ['wbcode', 'country_join_to_bonds', 'year', 'month']]
            self.e_years = self.e_years.rename(columns = {'country_join_to_bonds':'country'})
            self.e_years['date'] = self.e_years.apply(lambda row: datetime.strptime(f"{int(row.year)}-{int(row.month)}", '%Y-%m'), axis=1)
            # self.e['country_lower'] = self.e['country'].str.lower()
            self.e_years.to_csv(e_clean_outname)
            # e_years = elections.loc[(elections['leader_elect'] == 1), ['wbcode', 'country', 'year', 'month']]
            # e_years['date'] = e_years.apply(lambda row: datetime.strptime(f"{int(row.year)}-{int(row.month)}", '%Y-%m'), axis=1)
            # e['country_lower'] = e['country'].str.lower()

        print('Election Data Loaded.')



    def next_election_date(self, ctry, date):

        '''
        Returns the next election in a given country after a given date.
        '''

        e_dates = self.e_years[self.e_years['country'].str.lower() == ctry.lower()]
        diffs = e_dates['date'] - date
        min_diff = min(diffs[diffs >= pd.Timedelta('0 days')])
        date = e_dates[diffs == min_diff].iloc[0].loc['date']

        return(date)


    def find_right_next_date(self, tuple):
    # def find_right_next_date(tuple):

        '''
        Decides whether the next election date should be gotten or not for
        a particular country date combination, in cases where the date is
        too recent and there hasn't been an election or in which the country
        has no elections at all.
        '''

        ctry = tuple[0].lower()
        date = pd.to_datetime(tuple[1])

        country_dates = self.e_years[(self.e_years['country']).str.lower() == ctry.lower()]
        # country_dates = e_years[(e_years['country']).str.lower() == ctry.lower()]
        if pd.notnull(date):
            if country_dates['date'].shape[0] == 0: # if no elections at all
                output = pd.NaT
            elif date >= max(country_dates['date']): # if date after the latest election
                output = pd.NaT
            else: # if there's an election still to come
                diffs = country_dates['date'] - date
                min_diff = min(diffs[diffs > pd.Timedelta('0 days')])
                output = pd.to_datetime(country_dates[diffs == min_diff].iloc[0].loc['date'])
        else:
            output = pd.NaT

        return output



    def list_it(self, list_of_countries_and_dates):

        '''
        Get the next election for each date in the list of possible dates
        '''

        outlist = Parallel(n_jobs=self.cores)(delayed(self.find_right_next_date)(list_of_countries_and_dates.iloc[row,]) for row in tqdm(range(list_of_countries_and_dates.shape[0])))
        output = pd.DataFrame(data = outlist, columns = ['Next Election'])

        return(output)


    def most_recent_election_date(self, ctry, date):

        '''
        Returns the next election in a given country after a given date.
        '''

        e_dates = self.e_years[self.e_years['country'].str.lower() == ctry.lower()]
        diffs = e_dates['date'] - date
        min_diff = min(diffs[diffs > pd.Timedelta('0 days')])
        date = e_dates[diffs == min_diff].iloc[0].loc['date']

        return(date)


    def find_right_most_recent_date(self, tuple):
    # def find_right_most_recent_date(tuple):
        '''

        Decides whether the next election date should be gotten or not for
        a particular country date combination, in cases where the date is
        too recent and there hasn't been an election or in which the country
        has no elections at all.
        '''

        ctry = tuple[0].lower()
        date = pd.to_datetime(tuple[1])

        country_dates = self.e_years[(self.e_years['country']).str.lower() == ctry.lower()]
        # country_dates = e_years[(e_years['country']).str.lower() == ctry.lower()]
        if pd.notnull(date):
            if country_dates['date'].shape[0] == 0: # if no elections at all
                output = pd.NaT
            elif date <= min(country_dates['date']): # if date is before the earlies latest election
                output = pd.NaT
            else: # if there's an election still to come
                diffs = country_dates['date'] - date
                min_diff = max(diffs[diffs <= pd.Timedelta('0 days')])
                output = pd.to_datetime(country_dates[diffs == min_diff].iloc[0].loc['date'])
        else:
            output = pd.NaT

        return output


    def list_it_back(self, list_of_countries_and_dates):

        '''
        Get the next election for each date in the list of possible dates
        '''

        outlist = Parallel(n_jobs=self.cores)(delayed(self.find_right_most_recent_date)(list_of_countries_and_dates.iloc[row,]) for row in tqdm(range(list_of_countries_and_dates.shape[0])))
        output = pd.DataFrame(data = outlist, columns = ['Next Election'])

        return(output)


    def build_issue_data(self, ctry, x):
    # def build_issue_data(ctry, x):
        '''
        gets all the variables for a particular date and country combination.
        '''
        # ctry = 'AUSTRIA'
        # ctry_df = debt.s[debt.s['Country (Full Name)'] == ctry].reset_index(drop = True)
        # x = 2368
        ctry_df = self.s[self.s['Country (Full Name)'] == ctry].reset_index(drop = True)
        # ctry_df = ctry_df.iloc[x,:]

        out = ctry_df.iloc[x,:].to_dict()

        out['Issue Date'] = parse(str(ctry_df.iloc[x,:]['Issue Date'])) if str(ctry_df.iloc[x,:]['Issue Date']) != 'nan' else np.nan
        # out['Issue Date'] = parse(str(ctry_df['Issue Date'])) if str(ctry_df['Issue Date']) != 'nan' else np.nan
        out['Issue Date'] = pd.to_datetime(out['Issue Date'])

        out['Maturity Date'] = parse(str(ctry_df.iloc[x,:]['Maturity Date'])) if str(ctry_df.iloc[x,:]['Maturity Date']) != 'nan' else np.nan
        out['Maturity Date'] = pd.to_datetime(out['Maturity Date'])

        out['Next Election After Issuance'] = self.find_right_next_date((out['Country (Full Name)'], out['Issue Date']))
        out['Next Election After Maturity'] = self.find_right_next_date((out['Country (Full Name)'], out['Maturity Date']))

        # out['Next Election After Issuance'] = debt.find_right_next_date((out['Country (Full Name)'], out['Issue Date']))
        # out['Next Election After Maturity'] = debt.find_right_next_date((out['Country (Full Name)'], out['Maturity Date']))

        out['Days Issuance to Next Election'] = out['Next Election After Issuance'] - out['Issue Date']
        out['Days Maturity to Next Election'] = out['Next Election After Maturity'] - out['Maturity Date']

        out['Most Recent Election Before Issuance'] = self.find_right_most_recent_date((out['Country (Full Name)'], out['Issue Date']))
        out['Most Recent Election Before Maturity'] = self.find_right_most_recent_date((out['Country (Full Name)'], out['Maturity Date']))

        # out['Most Recent Election Before Issuance'] = debt.find_right_most_recent_date((out['Country (Full Name)'], out['Issue Date']))
        # out['Most Recent Election Before Maturity'] = debt.find_right_most_recent_date((out['Country (Full Name)'], out['Maturity Date']))

        out['Days Issuance since Last Election'] = out['Most Recent Election Before Issuance'] - out['Issue Date']
        out['Days Maturity since Last Election'] = out['Maturity Date'] - out['Most Recent Election Before Maturity']

        out['Mat.to.Election<90d'] = 1 if out['Next Election After Maturity'] - out['Maturity Date'] < pd.Timedelta('90 days') else 0
        out['Mat.to.Election<180d'] = 1 if out['Next Election After Maturity'] - out['Maturity Date'] < pd.Timedelta('180 days') else 0

        out['Issue.to.Election<90d'] = 1 if out['Next Election After Issuance'] - out['Issue Date'] < pd.Timedelta('90 days') else 0
        out['Issue.to.Election<180d'] = 1 if out['Next Election After Issuance'] - out['Issue Date'] < pd.Timedelta('180 days') else 0

        out['Mat.Since.Election<90d'] = 1 if out['Most Recent Election Before Maturity'] - out['Maturity Date'] < pd.Timedelta('90 days') else 0
        out['Mat.Since.Election<180d'] = 1 if out['Most Recent Election Before Maturity'] - out['Maturity Date'] < pd.Timedelta('180 days') else 0

        out['Issue.Since.Election<90d'] = 1 if out['Most Recent Election Before Issuance'] - out['Issue Date'] < pd.Timedelta('90 days') else 0
        out['Issue.Since.Election<180d'] = 1 if out['Most Recent Election Before Issuance'] - out['Issue Date'] < pd.Timedelta('180 days') else 0

        # filter the country df to only the issuances that have maturity date before
        # the next election. Then sum them up.

        out['Months.To.Election'] = (out['Next Election After Issuance'].year - out['Issue Date'].year) * 12 + (out['Next Election After Issuance'].month - out['Issue Date'].month)
        out['Months.Since.Election'] = -1 * ((out['Most Recent Election Before Issuance'].year - out['Issue Date'].year) * 12 + (out['Most Recent Election Before Issuance'].month - out['Issue Date'].month))

        out['Months.Mat.Next.Election'] = (out['Next Election After Issuance'].year - out['Maturity Date'].year) * 12 + (out['Next Election After Issuance'].month - out['Maturity Date'].month)

        out['Months.Mat.To.Election'] = (out['Next Election After Maturity'].year - out['Maturity Date'].year) * 12 + (out['Next Election After Maturity'].month - out['Maturity Date'].month)
        out['Months.Mat.Since.Election'] = -1 * ((out['Most Recent Election Before Maturity'].year - out['Maturity Date'].year) * 12 + (out['Most Recent Election Before Maturity'].month - out['Maturity Date'].month))

        out = pd.DataFrame.from_dict(out, orient = 'index').T
        return out


    def build_country_issue_data(self, ctry):
    # def build_country_issue_data(ctry):
        '''
        load all the issue data for one particular country.
        '''
        outname = self.dir + '/Tim_Code/Output_Data/cmc_issue_data_'+ ctry + '.csv'
        if len([f for f in listdir(self.dir + '/Tim_Code/Output_Data') if f.endswith('issue_data_'+ ctry + '.csv')]) > 0:
            out = pd.read_csv(outname)
        else:
            ctry_df = self.s[self.s['Country (Full Name)'] == ctry]
            # ctry = 'AUSTRIA'
            # ctry_df = s[s['Country (Full Name)'] == ctry]
            outlist = Parallel(n_jobs=self.cores)(delayed(self.build_issue_data)(ctry, x) for x in tqdm(range(ctry_df.shape[0])))
            # outlist = Parallel(n_jobs=int(os.cpu_count()))(delayed(build_issue_data)(ctry, x) for x in tqdm(range(ctry_df.shape[0])))
            out = pd.concat(outlist, ignore_index = True)
            out.to_csv(outname)

        return out


    def load_issue_data(self):

        '''
        load data about bond issuances and do some manipulations.
        '''
        dir = "C:/Users/trmcd/Dropbox/Debt Issues and Elections"
        s_outname = dir + '/Tim_Code/Output_Data/Issues/bond_issuances_py.csv'
        s = pd.read_csv(s_outname, index_col = 0)


        s_outname = self.dir + '/Tim_Code/Output_Data/bond_issuances_py.csv'
        # First, check and see if the file exists already.
        if len([f for f in listdir(self.dir + '/Tim_Code/Output_Data') if f.endswith('bond_issuances_py.csv')]) > 0:
            self.s = pd.read_csv(s_outname, index_col = 0)
        else:
            self.s = pd.read_excel(self.dir + "/Tim_Code/Bloomberg Sovereign Bond Data_updated2019_TM.xlsx") #low_memory = False)
            self.s = self.s.dropna(subset=['Country (Full Name)'])
            # TODO: should we exclude all NA Issue dates for bonds issued before 1990?
            # get the data for each country.
            outlist = Parallel(n_jobs=self.cores)(delayed(self.build_country_issue_data)(ctry) for ctry in tqdm(self.s['Country (Full Name)'].unique()))
            self.s = pd.concat(outlist, ignore_index = True)

            # i want a maturity length bucket
            # self.s = self.s[self.s['Maturity (Yrs)'] < 10]
            self.s = self.s[~pd.isnull(self.s['Maturity (Yrs)'])].reset_index(drop = True)
            self.s = self.s[~pd.isnull(self.s['Maturity Date'])].reset_index(drop = True)
            self.s['Maturity Date'] = [self.s['Maturity Date'].iloc[x] if self.s['Maturity (Yrs)'].iloc[x] > 0 else '20' + self.s['Maturity Date'].iloc[x][2:] for x in range(self.s.shape[0])]
            self.s['Maturity Date'] = [x if self.s['Maturity (Yrs)'].iloc[x] > 0 else '20' + self.s['Maturity Date'].iloc[x][2:] for x in self.s['Maturity Date']]

            def monthsdiff(enddate, startdate):
                enddate = dateparser.parse(enddate)
                startdate = dateparser.parse(startdate)
                return (enddate.year - startdate.year) * 12 + (enddate.month - startdate.month)

            self.s['Maturity (Months)'] = Parallel(n_jobs=self.cores)(delayed(monthsdiff)(enddate = self.s['Maturity Date'].iloc[row], startdate = self.s['Issue Date'].iloc[row]) for row in tqdm(range(self.s.shape[0])))

            # TODO: make closest election overall, regardless of before/after.
            # s_outname = self.dir + '/Tim_Code/bond_issuances_py.csv'
            self.s.to_csv(s_outname)

        manual_dtypes = {'Cpn': np.float64,
            'Issue Date': np.datetime64,
            'Maturity Date': np.datetime64,
            'Mty Type': str,
            'Price at Issue': float,
            'Yld to Mty (Ask)': float,
            'Yld to Mty (Bid)': float,
            'Yld to Mty (Mid)': float,
            'Inflation-Linked Note': str,
            'Is Retail Bond': str,
            'Was Bond Tapped': str,
            'Curr': str,
            'Days since Original Issue': np.float64,
            'Next Election After Maturity': str,
            'Most Recent Election Before Maturity': str,
            'Days Issuance to Next Election': str,
            'Days Issuance since Last Election': str,
            'Days Maturity to Next Election': str,
            'Days Maturity since Last Election': str,
            'Mat.to.Election<90d': np.int64,
            'Mat.to.Election<180d': np.int64,
            'Mat.Since.Election<90d': np.int64,
            'Mat.Since.Election<180d': np.int64,
            'Issue.to.Election<90d': np.int64,
            'Issue.to.Election<180d': np.int64,
            'Issue.Since.Election<90d': np.int64,
            'Issue.Since.Election<180d': np.int64,
            'Months.Since.Election': np.float64,
            'Months.To.Election': np.float64,
            'Months.Mat.To.Election': np.float64,
            'Months.Mat.Next.Election': np.float64,
            'Months.Mat.Since.Election': np.float64
            }

        self.s.replace('#N/A Field Not Applicable', np.NaN, regex = True, inplace = True)
        self.s['Price at Issue'].replace('-', np.NaN, regex = True, inplace = True)
        self.s.astype(manual_dtypes).dtypes
        self.s['Issue Date'] = pd.to_datetime(self.s['Issue Date'])
        self.s['Maturity Date'] = pd.to_datetime(self.s['Maturity Date'])
        # self.s['Mat.LT.6Y'] = [1 if (self.s.loc[x,'Maturity Date'] - self.s.loc[x,'Issue Date']).days < (365.25 * 6) else 0 for x in range(self.s.shape[0])]

        self.s.to_csv(s_outname)

        # Create a list of the unique month-year combinations present in the dataset
        self.unique_dates = self.s[pd.notnull(self.s['Issue Date'])]['Issue Date'].unique()
        self.unique_dates = [pd.to_datetime(str(x)).strftime('%Y-%m') for x in self.unique_dates]
        self.unique_dates = list(set(self.unique_dates))
        self.unique_dates.sort()
        self.unique_dates = [pd.to_datetime(x) for x in self.unique_dates]

        print('Issue Data Loaded.')



    def find_currency_category(self, ctry, curr):
        '''
        Given a country - currency combination, this function
        finds whether that combination is denominated in local currency (LC),
        foreign currency (FC), USD, or EUR.

        This is very sensitive to country name spelling and completeness.
        e.g. Mali or Mali Republic. It should be fine now, but is worth keeping
        an eye on.
        self.currencies.loc[(self.currencies['Country'] == 'MALI'), ['Country', 'Code']]
        '''

        ctry = ctry.upper()
        curr = curr.upper()
        temp = self.currencies.loc[(self.currencies['Country'] == ctry), ['Country', 'Code']]
        # Do EUR and USD first, that way EU and US don't have "LC" as their
        # currency types.
        if curr == 'EUR':
            type = 'EUR'
        elif curr == 'USD':
            type = 'USD'
        elif curr in list(temp['Code']):
            type = 'LC' # 'Local Currency'
        else:
            type = 'FC' # 'Foreign Currency'
        return type



    def get_currencies(self):
        '''
        this function blows out the issuances by the four currency possibilities:
        EUR, USD, LC, FC.

        This function creates two objects. The first (self.g) includes each combination
        of the issuing country, the issuing currency, and the category of
        currency with respect to that country in the form of EUR (Euro), USD
        (US Dollar), LC (Local Currency), or FC (Foreign Currency). It's at the
        granularity of country-month-currency. All columns in other objects will
        be dissected accordingly.

        The second is self.g blown out to be at the country-date-currency level,
        where each country-date combination has the number of records of the number
        of currencies that the country ever issued debt in.
        '''

        print('getting country month curr combinations')

        # If the document exists in the directory already
        if len([f for f in listdir(self.dir + '/Tim_Code') if f.endswith('currency_categories.csv')]) > 0:
            s_outname = self.dir + '/Tim_Code/currency_categories.csv'
            self.g = pd.read_csv(s_outname, index_col = 0)

        else:
            self.issued_country_currency_combinations = self.s[['Country (Full Name)','Curr']].drop_duplicates()
            tuple_list = list(zip(self.issued_country_currency_combinations['Country (Full Name)'], self.issued_country_currency_combinations['Curr']))
            c = Parallel(n_jobs = self.cores)(delayed(self.find_currency_category)(x[0], x[1]) for x in tqdm(tuple_list))
            self.g = pd.DataFrame(list(zip([x[0] for x in tuple_list],[x[1] for x in tuple_list],c)))
            self.g.to_csv(os.getcwd() + '/Tim_Code/currency_categories.csv')
            cols = ['Country', 'Curr', 'Curr_Type']
            self.g.columns = cols

        # Now, make a new document consisting of the unique combinations of
        # month, country, curr, and currtype.
        if len([f for f in listdir(self.dir + '/Tim_Code') if f.endswith('country_month_currency_combinations.csv')]) > 0:
            ctry_month_curr_filename = self.dir + '/Tim_Code/country_month_currency_combinations.csv'
            self.ctry_month_curr = pd.read_csv(ctry_month_curr_filename, index_col = 0)
            self.ctry_month_curr = self.ctry_month_curr[['Country (Full Name)', 'Date', 'Curr', 'Curr_Type']].drop_duplicates()
        # Now, make a new document consisting of the unique combinations of
        # month, country, curr, and currtype.
        else:
            outlist = []
            for ctry in self.s['Country (Full Name)'].unique():
                w = self.g[self.g['Country'] == ctry].drop_duplicates()
                temp = pd.concat([pd.DataFrame(list(zip(self.unique_dates, [w.iloc[x,0]] * len(self.unique_dates), [w.iloc[x,1]] * len(self.unique_dates), [w.iloc[x,2]] * len(self.unique_dates))), columns=['Date', 'Country (Full Name)','Curr', 'Curr_Type']) for x in range(w.shape[0])], ignore_index = True)
                outlist.append(temp)
            self.ctry_month_curr = pd.concat(outlist, ignore_index = True)
            self.ctry_month_curr['Date'] = [pd.to_datetime(x) for x in self.ctry_month_curr['Date']]
            ctry_month_curr_filename = self.dir + '/Tim_Code/country_month_currency_combinations.csv'
            # self.ctry_month_curr.to_csv(ctry_month_curr_filename)

        print('Currency Data Loaded.')



    def get_date_vars(self, date_ctry_curr_currtype_quad):

        """
        Next, show what amt of debt matured during each country-month.
        DVs:
            Pct of debt with yy year maturity time frame issued as a pct of overall debt issued during t country-month
            Curr makeup of Debt
            term length
            yields
            coupon
        Do investors demand higher yield to change maturity profile?
            Currency profile of debt
            Borrowing terms (e.g. lower interest rates if shorter maturity)?

        This function takes the most time to run for two reasons:
        First, it does the math by individual date.
        Second, it's a bit time-intensive for the calculation.
        """
        # date_ctry_curr_currtype_quad = ('1990-01-19', 'AUSTRIA', 'EUR', 'EUR')

        # print(date_ctry_curr_currtype_quad)

        date = pd.to_datetime(date_ctry_curr_currtype_quad[1])
        ctry = date_ctry_curr_currtype_quad[0]
        curr = date_ctry_curr_currtype_quad[2]
        currtype = date_ctry_curr_currtype_quad[3]
        # mty = date_ctry_curr_currtype_quad[4]

        dc_df = pd.DataFrame(columns = ['Country'], data = [None])
        dc_df['Country'] = ctry
        dc_df['Date'] = date
        dc_df['Curr'] = curr
        dc_df['Curr_Type'] = currtype
        # dc_df['Mty'] = mty

        # ctry_df = debt.s[(debt.s['Country (Full Name)'] == ctry)]
        ctry_df = self.s[(self.s['Country (Full Name)'] == ctry)]
        # print(ctry_df.head())

        # ctry = 'BELGIUM'
        # date = pd.to_datetime('2001-01-20')
        # curr = 'EUR'
        # currtype = 'EUR'

        # ctry_df = pd.read_csv('C:/Users/trmcd/Dropbox/Debt Issues and Elections/Tim_Code/Output_Data/issue_data_BELGIUM.csv', index_col=0)
        # ctry_df['Maturity Date'] = pd.to_datetime(ctry_df['Maturity Date'])
        # ctry_df['Issue Date'] = pd.to_datetime(ctry_df['Issue Date'])
        # ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Maturity Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m'))]
        # ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Maturity Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) ]['Amount Issued'].sum(skipna = True)

        # dc_df['Amt.Issued.Mat.to.Election<12m']
        # ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m'))].sample(15)
        # ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.To.Election'] <= 12)]['Amount Issued'].sum(skipna = True)
        # dc_df['Pct.Issued.Mat.to.Election<12m'] = dc_df['Amt.Issued.Mat.to.Election<12m'].divide(dc_df['Amt.Issued'])

        # TODO: Add yield at issue, coupon at issue, term length, etc.
        dc_df['Amt.Matured'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Maturity Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) ]['Amount Issued'].sum(skipna = True)
        dc_df['Amt.Issued'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) ]['Amount Issued'].sum(skipna = True)

        dc_df['Amt.Issued.Mat.to.Election<90d'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Maturity Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Mat.to.Election<90d'] == 1)]['Amount Issued'].sum(skipna = True)
        dc_df['Amt.Issued.Mat.to.Election<180d'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Maturity Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Mat.to.Election<180d'] == 1)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.to.Election<90d'] = dc_df['Amt.Issued.Mat.to.Election<90d'].divide(dc_df['Amt.Issued'])
        dc_df['Pct.Issued.Mat.to.Election<180d'] = dc_df['Amt.Issued.Mat.to.Election<180d'].divide(dc_df['Amt.Issued'])

        dc_df['Amt.Issued.Mat.Since.Election<90d'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Maturity Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Mat.Since.Election<90d'] == 1)]['Amount Issued'].sum(skipna = True)
        dc_df['Amt.Issued.Mat.Since.Election<180d'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Maturity Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Mat.Since.Election<180d'] == 1)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.Since.Election<90d'] = dc_df['Amt.Issued.Mat.Since.Election<90d'].divide(dc_df['Amt.Issued'])
        dc_df['Pct.Issued.Mat.Since.Election<180d'] = dc_df['Amt.Issued.Mat.Since.Election<180d'].divide(dc_df['Amt.Issued'])

        # TODO: sum up the amount of debt issued that comes due within 12 months of the next election
        # ctry_df[(debt.s['Maturity Date'] < pd.to_datetime(x['Maturity Date'][0]) + relativedelta(months = n[1]))]['Amt.Issued']
        # here I want the distance between the maturity date and the next election, not the
        # issuance date and the next election. so I need to do mat.to.election
        # for a given country and currency, the amt issued on a given date t that matures within T months of the election.

        dc_df['Amt.Issued.Mat.to.Election<12m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.To.Election'] <= 12)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.to.Election<12m'] = dc_df['Amt.Issued.Mat.to.Election<12m'].divide(dc_df['Amt.Issued'])

        dc_df['Amt.Issued.Mat.to.Election<6m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.To.Election'] <= 6)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.to.Election<6m'] = dc_df['Amt.Issued.Mat.to.Election<6m'].divide(dc_df['Amt.Issued'])

        dc_df['Amt.Issued.Mat.to.Election<3m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.To.Election'] <= 3)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.to.Election<3m'] = dc_df['Amt.Issued.Mat.to.Election<3m'].divide(dc_df['Amt.Issued'])

        dc_df['Amt.Issued.Mat.to.Election<1m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.To.Election'] <= 1)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.to.Election<1m'] = dc_df['Amt.Issued.Mat.to.Election<1m'].divide(dc_df['Amt.Issued'])

        dc_df['Amt.Issued.Mat.to.Election<0m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.To.Election'] <= 0)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.to.Election<0m'] = dc_df['Amt.Issued.Mat.to.Election<0m'].divide(dc_df['Amt.Issued'])


        dc_df['Amt.Issued.Mat.Next.Election<12m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.Next.Election'] <= 12)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.Next.Election<12m'] = dc_df['Amt.Issued.Mat.Next.Election<12m'].divide(dc_df['Amt.Issued'])

        dc_df['Amt.Issued.Mat.Next.Election<6m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.Next.Election'] <= 6)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.Next.Election<6m'] = dc_df['Amt.Issued.Mat.Next.Election<6m'].divide(dc_df['Amt.Issued'])

        dc_df['Amt.Issued.Mat.Next.Election<3m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.Next.Election'] <= 3)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.Next.Election<3m'] = dc_df['Amt.Issued.Mat.Next.Election<3m'].divide(dc_df['Amt.Issued'])

        dc_df['Amt.Issued.Mat.Next.Election<1m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.Next.Election'] <= 1)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.Next.Election<1m'] = dc_df['Amt.Issued.Mat.Next.Election<3m'].divide(dc_df['Amt.Issued'])

        dc_df['Amt.Issued.Mat.Next.Election<0m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.Next.Election'] <= 0)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.Next.Election<0m'] = dc_df['Amt.Issued.Mat.Next.Election<3m'].divide(dc_df['Amt.Issued'])


        dc_df['Amt.Issued.Mat.Since.Election<12m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.Since.Election'] <= 12)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.Since.Election<12m'] = dc_df['Amt.Issued.Mat.Since.Election<12m'].divide(dc_df['Amt.Issued'])

        dc_df['Amt.Issued.Mat.Since.Election<6m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.Since.Election'] <= 6)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.Since.Election<6m'] = dc_df['Amt.Issued.Mat.Since.Election<6m'].divide(dc_df['Amt.Issued'])

        dc_df['Amt.Issued.Mat.Since.Election<3m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.Since.Election'] <= 3)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.Since.Election<3m'] = dc_df['Amt.Issued.Mat.Since.Election<3m'].divide(dc_df['Amt.Issued'])

        dc_df['Amt.Issued.Mat.Since.Election<1m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.Since.Election'] <= 1)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.Since.Election<1m'] = dc_df['Amt.Issued.Mat.Since.Election<1m'].divide(dc_df['Amt.Issued'])

        dc_df['Amt.Issued.Mat.Since.Election<0m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.Since.Election'] <= 0)]['Amount Issued'].sum(skipna = True)
        dc_df['Pct.Issued.Mat.Since.Election<0m'] = dc_df['Amt.Issued.Mat.Since.Election<0m'].divide(dc_df['Amt.Issued'])

        # n_issues = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m'))].shape[0]
        #
        # dc_df['Months.To.Election<12'] = 1 if ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m'))]['Months.To.Election'].unique()[0] < 13 else 0
        # dc_df['Pct.Issues.Mat.To.Election<12m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.To.Election'] < 13)].shape[0] / n_issues
        #
        # dc_df['Months.To.Election<6'] = 1 if ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m'))]['Months.To.Election'].unique()[0] < 7 else 0
        # dc_df['Pct.Issues.Mat.To.Election<6m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.To.Election'] < 7)].shape[0] / n_issues
        #
        # dc_df['Months.To.Election<3'] = 1 if ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m'))]['Months.To.Election'].unique()[0] < 4 else 0
        # dc_df['Pct.Issues.Mat.To.Election<3m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.To.Election'] < 4)].shape[0] / n_issues
        #
        # dc_df['Months.To.Election<1'] = 1 if ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m'))]['Months.To.Election'].unique()[0] < 2 else 0
        # dc_df['Pct.Issues.Mat.To.Election<1m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.To.Election'] < 2)].shape[0] / n_issues
        #
        # dc_df['Months.To.Election<0'] = 1 if ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m'))]['Months.To.Election'].unique()[0] < 1 else 0
        # dc_df['Pct.Issues.Mat.To.Election<0m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.To.Election'] < 1)].shape[0] / n_issues

        ##

        # For each issue, code as whether maturity date is within the pre-election window (actually, within *any* pre-election window….
        # a 10 year bond issued in 2008, when there’s an election in 2013 and again in 2018, falls into the second pre election window
        # but not the first…). Sum the value of all issues in each of the two buckets. And then our “bucket” is election bucket as a
        # share of all issues that month. One other issue: we are going to have some zero values that result not from 0 in a bucket,
        # but from zero issues in either bucket [that is, months with no issues at all are missing, not zero, just need to be careful of this]

        # dc_df['Months.Since.Election<12'] = 1 if ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m'))]['Months.Since.Election'].unique()[0] < 13 else 0
        # dc_df['Pct.Issues.Mat.Since.Election<12m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.Since.Election'] < 13)].shape[0] / n_issues
        #
        # dc_df['Months.Since.Election<6'] = 1 if ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m'))]['Months.Since.Election'].unique()[0] < 7 else 0
        # dc_df['Pct.Issues.Mat.Since.Election<6m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.Since.Election'] < 7)].shape[0] / n_issues
        #
        # dc_df['Months.Since.Election<3'] = 1 if ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m'))]['Months.Since.Election'].unique()[0] < 4 else 0
        # dc_df['Pct.Issues.Mat.Since.Election<3m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.Since.Election'] < 4)].shape[0] / n_issues
        #
        # dc_df['Months.Since.Election<1'] = 1 if ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m'))]['Months.Since.Election'].unique()[0] < 2 else 0
        # dc_df['Pct.Issues.Mat.Since.Election<1m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.Since.Election'] < 2)].shape[0] / n_issues
        #
        # dc_df['Months.Since.Election<0'] = 1 if ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m'))]['Months.Since.Election'].unique()[0] < 1 else 0
        # dc_df['Pct.Issues.Mat.Since.Election<0m'] = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) & (ctry_df['Months.Mat.Since.Election'] < 1)].shape[0] / n_issues

        dc_df['Mat.Issue.Ratio'] = dc_df['Amt.Matured'].divide(dc_df['Amt.Issued'])
        # calculate average time to maturity for the outstanding debt for the given date
        p = ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'] <= datetime.strftime(date, '%Y-%m-%d')) & (ctry_df['Maturity Date'] >= datetime.strftime(date, '%Y-%m-%d')) ].copy()
        p['Remaining.Time'] = pd.NaT if p.shape[0] == 0 else pd.to_datetime(ctry_df['Maturity Date']) - pd.to_datetime(ctry_df['Issue Date'])
        p['Remaining.Time'] = [x.days if pd.notnull(x) else x for x in p['Remaining.Time']]
        p['weight.at.issue'] = (p['Amount Issued'].divide(p['Amount Issued'].sum(skipna = True))) * (p['Maturity (Yrs)'] * 365.25)
        p['weight.remaining'] = (p['Amount Issued'].divide(p['Amount Issued'].sum(skipna = True))) * p['Remaining.Time']

        dc_df['Avg.TTM.at.Issuance.of.Outstanding'] = p['weight.at.issue'].sum(skipna = True)
        dc_df['Avg.TTM.Remaining.of.Outstanding'] = p['weight.remaining'].sum(skipna = True)

        return(dc_df)



    def get_country_vars(self, ctry):
        '''
        The below takes the country-date-curr-currtype combinations and
        gets all the variables per date.
        '''

        print(f'getting country vars {ctry}')

        # this input needs to have the maturity lengths in it too.
        ctry_df = self.ctry_month_curr[self.ctry_month_curr['Country (Full Name)'] == ctry]
        ctry_df['Date'] = pd.to_datetime(ctry_df['Date'])
        temp = Parallel(n_jobs = self.cores)(delayed(self.get_date_vars)(ctry_df.iloc[x,]) for x in tqdm(range(ctry_df.shape[0])))

        ######

        # temp = pd.concat([pd.DataFrame(list(zip(debt.unique_dates, [w.iloc[x,0]] * len(debt.unique_dates), [w.iloc[x,1]] * len(debt.unique_dates), [w.iloc[x,2]] * len(debt.unique_dates))), columns=['Date', 'Country (Full Name)','Curr', 'Curr_Type']) for x in range(w.shape[0])], ignore_index = True)
        # ctry_maturities = sorted(debt.s[debt.s['Country (Full Name)'] == ctry]['Maturity (Months)'].unique())
        # len(ctry_maturities)

        # temp = pd.concat([pd.DataFrame(list(zip(self.unique_dates, [w.iloc[x,0]] * len(self.unique_dates), [w.iloc[x,1]] * len(self.unique_dates), [w.iloc[x,2]] * len(self.unique_dates), ctry_mtys)), columns=['Date', 'Country (Full Name)','Curr', 'Curr_Type', 'Maturity (Months)']) for x in range(w.shape[0])], ignore_index = True)
        # outlist.append(temp)
        # self.ctry_month_curr = pd.concat(outlist, ignore_index = True)

        ###

        ######

        country_data = pd.concat(temp, ignore_index = True)
        country_data['Date'] = [x.replace(day=1) for x in country_data['Date']]
        country_data['Month'] = [x.month for x in country_data['Date']]
        country_data['Year'] = [x.year for x in country_data['Date']]
        country_data['Country_Lower'] = country_data['Country'].str.lower()

        return(country_data)



    def get_country_data_all(self, ctry):

        '''
        This function adds the election data to the issuance data for a country.
        '''

        print('get country data all ')
        print(ctry)
        df_temp_filename = self.dir + '/Tim_Code/Output_Data/' + ctry + '_cmc_intermediate_output_' + self.pull_date + '.csv'
        print(df_temp_filename)
        country_vars_temp_filename = self.dir + '/Tim_Code/Output_Data/' + ctry + '_cmc_regression_data_' + self.pull_date + '.csv'
        print(country_vars_temp_filename)

        # see if the country's regression data exists already.
        if len([f for f in listdir(self.dir + '/Tim_Code/Output_Data') if f.endswith(ctry + '_cmc_regression_data_' + self.pull_date + '.csv')]) <= 0:
            # if there is already an intermediate file for this country that
            # has election dates etc, use it
            print('first if loop')
            if len([f for f in listdir(self.dir + '/Tim_Code/Output_Data') if f.endswith(ctry + '_cmc_intermediate_output_' + self.pull_date + '.csv')]) < 0:
                print('second if loop')
                df = pd.read_csv(df_temp_filename, index_col = 0)
            else:
                print('else loop')
                df = pd.DataFrame(columns = ['Date'], data = self.unique_dates)
                df['Country'] = ctry
                df['Next.Election.Date'] = self.list_it(df[['Country', 'Date']])
                df['Days.to.Next.Election'] = [pd.NaT if pd.isnull(df.iloc[x]['Next.Election.Date']) else df.iloc[x]['Next.Election.Date'] - df.iloc[x]['Date'] for x in range(df.shape[0])]
                df['Days.to.Next.Election'] = [x.days if pd.notnull(x) else x for x in df['Days.to.Next.Election']]
                df['Days.to.Election<90d'] = [1 if df.iloc[x]['Next.Election.Date'] - df.iloc[x]['Date'] < pd.Timedelta('90 days') else 0 for x in range(df.shape[0])]
                df['Days.to.Election<180d'] = [1 if df.iloc[x]['Next.Election.Date'] - df.iloc[x]['Date'] < pd.Timedelta('180 days') else 0 for x in range(df.shape[0])]

                # df['Months.To.Election'] = (df['Next.Election.Date'].year - df['Date'].year) * 12 + (df['Next.Election.Date'].month - df['Date'].month)
                df['Months.To.Election'] = [(df.loc[x,'Next.Election.Date'].year - df.loc[x,'Date'].year) * 12 + (df.loc[x,'Next.Election.Date'].month - df.loc[x,'Date'].month) for x in range(df.shape[0])]

                df['Most.Recent.Election.Date'] = self.list_it_back(df[['Country', 'Date']])
                df['Days.Since.Most.Recent.Election'] = [pd.NaT if pd.isnull(df.iloc[x]['Most.Recent.Election.Date']) else df.iloc[x]['Date'] - df.iloc[x]['Most.Recent.Election.Date'] for x in range(df.shape[0])]
                df['Days.Since.Most.Recent.Election'] = [x.days if pd.notnull(x) else x for x in df['Days.Since.Most.Recent.Election']]
                df['Days.Since.Election<90d'] = [1 if df.iloc[x]['Most.Recent.Election.Date'] - df.iloc[x]['Date'] < pd.Timedelta('90 days') else 0 for x in range(df.shape[0])]
                df['Days.Since.Election<180d'] = [1 if df.iloc[x]['Most.Recent.Election.Date'] - df.iloc[x]['Date'] < pd.Timedelta('180 days') else 0 for x in range(df.shape[0])]

                # df['Months.Since.Election'] = (df['Most.Recent.Election.Date'].year - df['Date'].year) * 12 + (df['Most.Recent.Election.Date'].month - df['Date'].month)
                df['Months.Since.Election'] = [(df.loc[x,'Most.Recent.Election.Date'].year - df.loc[x,'Date'].year) * 12 + (df.loc[x,'Most.Recent.Election.Date'].month - df.loc[x,'Date'].month) for x in range(df.shape[0])]

                df.to_csv(df_temp_filename)

            print('Getting Country Variables for ' + ctry)

            # k = pd.merge(left = df, right = self.get_country_vars(ctry), how = 'outer', on = ['Country', 'Date'])
            y = self.get_country_vars(ctry)
            y['Date'] = y['Date'].astype('string')
            df['Date'] = df['Date'].astype('string')
            k = df.merge(y, how = 'outer', on = ['Country', 'Date'])
            k_name = self.dir + '/Tim_Code/Output_Data/' + ctry + '_cmc_regression_data_' + self.pull_date + '.csv'
            k.to_csv(k_name)

        else:
            pass


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

        dpi = pd.read_csv(self.dir + '/Explanatory Vars/WB DPI/WB_DPI_2017_TM_20200615.csv', index_col = 0, low_memory = False)
        dpi = dpi.loc[:,~dpi.columns.duplicated()]

        print('Upon loading, elections dataset has ', len(self.e['country_join_to_bonds'].unique(
        )), 'countries. The date range is ', self.e['year'].min(), 'until ', self.e['year'].max(), '.')

        self.e = self.e.merge(dpi,
                              left_on = ['country', 'year', 'month'],
                              right_on = ['countryname', 'year', 'month'])
        self.e = self.e.loc[:,~self.e.columns.duplicated()]
        keep_cols = set([x for x in self.e.columns if not x.endswith('_x')])
        self.e = self.e[keep_cols]
        keep_cols2 = [x if not x.endswith('_y') else x.split('_y')[0] for x in self.e.columns]
        self.e.columns = keep_cols2
        self.e = self.e[set(keep_cols2)]
        self.e = self.e.loc[:,~self.e.columns.duplicated()]
        keep_cols3 = set([x for x in self.e.columns if not x.endswith('.1')])
        self.e = self.e[keep_cols3]
        self.e = self.e.sort_values(['country', 'year', 'month'])

        print('After merging DPI, elections dataset has ', len(self.e['country_join_to_bonds'].unique()), 'countries. The date range is ', self.e['year'].min(), 'until ', self.e['year'].max(),'.')

        # # Debt statistics variables come from the IMF EDS dataset, managed in eds_data_prep.py
        # eds = pd.read_csv(self.dir + '/Explanatory Vars/IMF EDS/IMF_EDS_2020_TM_20200616.csv', index_col = 0)
        # self.e = self.e.merge(eds,
        #                  left_on = ['country', 'year', 'month'],
        #                  right_on = ['Country Name', 'Year', 'Month'])

        # filter by only those countries that have regular competitive elections.
        # definitions in reg_elections_data_prep.py
        reg_elections = pd.read_csv(self.dir + '/Explanatory Vars/WB DPI/Regular_elections_WB_DPI_2017_TM_20200625.csv', index_col = 0)
        self.e = self.e.merge(reg_elections,
                              left_on = ['country'],
                              right_on = ['countryname'])

        print('After merging reg_elections, elections dataset has ', len(self.e['country_join_to_bonds'].unique(
        )), 'countries. The date range is ', self.e['year'].min(), 'until ', self.e['year'].max(), '.')

        self.out = self.out[self.out['reg_elections']
                            == 1].reset_index(drop=True)


        # polity = pd.read_csv(self.dir + '/Explanatory Vars/PolityIV/original/pv_for_debt_2020_10_12.csv', index_col = 0)
        # self.e = self.e.merge(polity,
        #                       left_on = ['country', 'year', 'month'],
        #                       right_on = ['country', 'year', 'month'])

        # macro = pd.read_csv(self.dir + '/Explanatory Vars/WB WDI/WB_macro_2019_TM_20200707.csv', index_col = 0)
        # self.e = self.e.merge(macro,
        #                       left_on = ['country', 'year'],
        #                       right_on = ['Country Name', 'Year'])

        # spx = pd.read_csv(self.dir + '/Explanatory Vars/SPX/SPX_historical_2020_TM_20200708.csv', index_col = 0)
        # self.e = self.e.merge(spx,
        #                       left_on = ['year', 'month'],
        #                       right_on = ['Year', 'Month'])

        vdem = pd.read_csv(self.dir + '/Explanatory Vars/VDem/VDem_elections_v10_TM_20200708.csv', index_col = 0)
        self.e = self.e.merge(vdem,
                              left_on = ['country', 'year'],
                              right_on = ['country_name', 'year'])

        print('After merging vdem, elections dataset has ', len(self.e['country_join_to_bonds'].unique()), 'countries. The date range is ', self.e['year'].min(), 'until ', self.e['year'].max(),'.')

        # add in interest rates etc.
        irs = pd.read_csv(self.dir + '/Explanatory Vars/Interest Rates/IRs_FFR_LIBOR_EURIBOR_TM_20200708.csv', index_col = 0)
        self.e = self.e.merge(irs,
                              left_on = ['year', 'month'],
                              right_on = ['Year', 'Month'])

        print('After merging irs, elections dataset has ', len(self.e['country_join_to_bonds'].unique()), 'countries. The date range is ', self.e['year'].min(), 'until ', self.e['year'].max(),'.')

        # add in sov credit rating.
        cr = pd.read_csv(self.dir + '/Explanatory Vars/Credit ratings/Sov_Credit_Rating.csv', index_col = 0)
        self.e = self.e.merge(cr,
                              left_on = ['country', 'year', 'month'],
                              right_on = ['country', 'year', 'month'])

        print('After merging credit ratings, elections dataset has ', len(self.e['country_join_to_bonds'].unique()), 'countries. The date range is ', self.e['year'].min(), 'until ', self.e['year'].max(),'.')

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
                                  left_on = ['Year', 'Month', 'Country'],
                                  right_on = ['year', 'month', 'country_join_to_bonds'])
                                  # left_on = ['Year', 'Month', 'Country_Lower'],
                                  # right_on = ['year', 'month', 'country_lower'])

        print('After merging with elections, out has ', len(self.out['Country'].unique()), 'countries. The date range is ', self.out['year'].min(), 'until ', self.out['year'].max(),'.')

        # order the columns how we want them and get rid of duplicates:
        final_cols = ['Country'
                        # ,'iso2code'
                        # 'wbcode'
                        # ,'regioncode'
                        ,'Date'
                        ,'year'
                        ,'month'
                        ,'Curr'
                        ,'Curr_Type'
                        # ,'Mty'
                        # 'Mat.LT.6Y'
                        ,'Next.Election.Date'
                        ,'Most.Recent.Election.Date'
                        # ,'Days.to.Next.Election'
                        # ,'Days.to.Election<90d'
                        # ,'Days.to.Election<180d'
                        # ,'Days.Since.Most.Recent.Election'
                        # ,'Days.Since.Election<90d'
                        # ,'Days.Since.Election<180d'

                        ,'Months.To.Election'
                        ,'Months.Since.Election'

                        ,'Amt.Matured'
                        ,'Amt.Issued'

                        ,'Amt.Issued.Mat.to.Election<90d'
                        ,'Amt.Issued.Mat.to.Election<180d'
                        ,'Pct.Issued.Mat.to.Election<90d'
                        ,'Pct.Issued.Mat.to.Election<180d'

                        ,'Amt.Issued.Mat.Since.Election<90d'
                        ,'Amt.Issued.Mat.Since.Election<180d'
                        ,'Pct.Issued.Mat.Since.Election<90d'
                        ,'Pct.Issued.Mat.Since.Election<180d'

                        ,'Amt.Issued.Mat.to.Election<12m'
                        ,'Pct.Issued.Mat.to.Election<12m'
                        ,'Amt.Issued.Mat.to.Election<6m'
                        ,'Pct.Issued.Mat.to.Election<6m'
                        ,'Amt.Issued.Mat.to.Election<3m'
                        ,'Pct.Issued.Mat.to.Election<3m'
                        ,'Amt.Issued.Mat.to.Election<1m'
                        ,'Pct.Issued.Mat.to.Election<1m'
                        ,'Amt.Issued.Mat.to.Election<0m'
                        ,'Pct.Issued.Mat.to.Election<0m'

                        ,'Amt.Issued.Mat.Next.Election<12m'
                        ,'Pct.Issued.Mat.Next.Election<12m'
                        ,'Amt.Issued.Mat.Next.Election<6m'
                        ,'Pct.Issued.Mat.Next.Election<6m'
                        ,'Amt.Issued.Mat.Next.Election<3m'
                        ,'Pct.Issued.Mat.Next.Election<3m'
                        ,'Amt.Issued.Mat.Next.Election<1m'
                        ,'Pct.Issued.Mat.Next.Election<1m'
                        ,'Amt.Issued.Mat.Next.Election<0m'
                        ,'Pct.Issued.Mat.Next.Election<0m'

                        ,'Amt.Issued.Mat.Since.Election<12m'
                        ,'Pct.Issued.Mat.Since.Election<12m'
                        ,'Amt.Issued.Mat.Since.Election<6m'
                        ,'Pct.Issued.Mat.Since.Election<6m'
                        ,'Amt.Issued.Mat.Since.Election<3m'
                        ,'Pct.Issued.Mat.Since.Election<3m'
                        ,'Amt.Issued.Mat.Since.Election<1m'
                        ,'Pct.Issued.Mat.Since.Election<1m'
                        ,'Amt.Issued.Mat.Since.Election<0m'
                        ,'Pct.Issued.Mat.Since.Election<0m'

                        # ,'Months.To.Election<12'
                        # ,'Pct.Issues.Mat.To.Election<12m'
                        # ,'Months.To.Election<6'
                        # ,'Pct.Issues.Mat.To.Election<6m'
                        # ,'Months.To.Election<3'
                        # ,'Pct.Issues.Mat.To.Election<3m'
                        # ,'Months.To.Election<1'
                        # ,'Pct.Issues.Mat.To.Election<1m'
                        # ,'Months.To.Election<0'
                        # ,'Pct.Issues.Mat.To.Election<0m'
                        #
                        # ,'Months.Since.Election<12'
                        # ,'Pct.Issues.Mat.Since.Election<12m'
                        # ,'Months.Since.Election<6'
                        # ,'Pct.Issues.Mat.Since.Election<6m'
                        # ,'Months.Since.Election<3'
                        # ,'Pct.Issues.Mat.Since.Election<3m'
                        # ,'Months.Since.Election<1'
                        # ,'Pct.Issues.Mat.Since.Election<1m'
                        # ,'Months.Since.Election<0'
                        # ,'Pct.Issues.Mat.Since.Election<0m'

                        ,'Mat.Issue.Ratio'
                        ,'Avg.TTM.at.Issuance.of.Outstanding'
                        ,'Avg.TTM.Remaining.of.Outstanding'

                        , 'vote_margin'                        # DPI columns
                        , 'system', 'yrcurnt', 'execrlc'
                        # 'Gross Ext. Debt Pos., Central Bank, All maturities, Debt Securities, Market Value, USD [DT.DOD.DECT.CD.MA.TD.MV.US]',
                        # 'Gross Ext. Debt Pos., General Government, All maturities, Debt Securities, Market Value, USD [DT.DOD.DECT.CD.GG.TD.MV.US]',
                        'v2x_mpi',
                        'Inflation (Consumer Prices, annual %)',
                        'GDPPC growth',
                        'SPX_Pct_Change_MoM',
                        'inv_grade',
                        'cr',
                        'LIBOR O/N',
                        'EURIBOR O/N',
                        'US_FFR'
                      ]


        self.out = self.out[[x for x in self.out.columns if x in final_cols]]

        print('the columns in out are ')
        print(self.out.shape)

        for col in ['Pct.Issued.Mat.to.Election<90d', 'Pct.Issued.Mat.to.Election<180d', 'Pct.Issued.Mat.Since.Election<90d', 'Pct.Issued.Mat.Since.Election<180d', 'Mat.Issue.Ratio']:
            if col in self.out.columns:
                self.out.loc[:,col] = [np.nan_to_num(x, posinf = 0, neginf = 0, copy = False) if (np.isinf(x) or np.isnan(x)) else x for x in self.out.loc[:,col]]

        self.out.loc[:,'Next.Election.Date'] = ['1900-01-01' if not isinstance(x, str) else x for x in self.out.loc[:,'Next.Election.Date']]
        self.out.loc[:,'Most.Recent.Election.Date'] = ['1900-01-01' if not isinstance(x, str) else x for x in self.out.loc[:,'Most.Recent.Election.Date']]

        self.out.loc[:,'Days.to.Next.Election'] = [np.nan_to_num(x, nan = -999, posinf = -999, neginf = -999, copy = False) if (np.isinf(x) or np.isnan(x)) else x for x in self.out.loc[:,'Days.to.Next.Election']]
        self.out.loc[:,'Days.Since.Most.Recent.Election'] = [np.nan_to_num(x, nan = -999, posinf = -999, neginf = -999, copy = False) if (np.isinf(x) or np.isnan(x)) else x for x in self.out.loc[:,'Days.Since.Most.Recent.Election']]

        for col in ['Gross Ext. Debt Pos., Central Bank, Long-term, Debt Securities, Market Value, USD [DT.DOD.DLXF.CD.MA.TD.MV.US]','Gross Ext. Debt Pos., Central Bank, All maturities, Debt Securities, Market Value, USD [DT.DOD.DECT.CD.MA.TD.MV.US]','Gross Ext. Debt Pos., Central Bank, Short-term, Debt Securities, Market Value, USD [DT.DOD.DSTC.CD.MA.TD.MV.US]','Gross Ext. Debt Pos., General Government, Short-term, Debt Securities, Market Value, USD [DT.DOD.DSTC.CD.GG.TD.MV.US]','Gross Ext. Debt Pos., General Government, Long-term, Debt Securities, Market Value, USD [DT.DOD.DLXF.CD.GG.TD.MV.US]','Gross Ext. Debt Pos., General Government, All maturities, Debt Securities, Market Value, USD [DT.DOD.DECT.CD.GG.TD.MV.US]']:
                    if col in self.out.columns:
                        self.out.loc[:,col] = [x if not x == '..' else np.nan for x in self.out.loc[:,col]]


        # self.out = self.out[self.out['Days.Since.Most.Recent.Election'] != -999]
        # restrict to only those records with a valid next recent election
        # x = x[x['Days.to.Next.Election'] != -999]
        # do a little data cleaning that will not be necessary in the next version of the dataset
        # self.out = self.out[(self.out['LIBOR O/N'] != -999) & (self.out['EURIBOR O/N'] != -999)]

        # dummy_list = ['system','execme', 'execrlc', 'prtyin','pr', 'pluralty','housesys','percent1','ssh','sensys']
        # dummies = pd.get_dummies(self.out[dummy_list]).fillna(0)

        # self.out = self.out[[col for col in self.out.columns if col not in ['LIBOR O/N', 'EURIBOR O/N']]]

        # drop columns that have any -999 entries.
        # drop_cols = [col for col in self.out.columns if self.out[self.out[col] == -999][col].shape[0]/self.out.shape[0] > 0]
        # self.out = self.out[[col for col in self.out.columns if col not in drop_cols]]
        # [(col, x[col].isnull().sum()/x.shape[0]) for col in x.columns]

        # print('before dropping strict, out is')
        # print(self.out.shape)

        # drop_cols = [x for x in self.out.columns if x.endswith('_strict')]
        # self.out = self.out[[col for col in self.out.columns if col not in drop_cols]].reset_index(drop = True)

        # print('after dropping strict, the columns in out are ')
        # print(self.out.shape)

        # do a little data cleaning that will not be necessary in the next version of the dataset
        # Debt statistics variables come from the IMF EDS dataset, managed in eds_data_prep.py

        # print('before scaling, out is')
        # print(self.out.shape)

        outname = self.dir + '/Tim_Code/Output_Data/cmc_regression_data_' + self.pull_date + '.csv'
        self.out.to_csv(outname)

        # scaler = MinMaxScaler()
        # not_scale_list = ['Country','Date','year', 'month', 'Next.Election.Date','Most.Recent.Election.Date','Curr','Curr_Type'] + [x for x in dpi.columns]
        # scale_cols = [x for x in self.out.columns if x not in not_scale_list]
        # transformed = pd.DataFrame(scaler.fit_transform(self.out[scale_cols]), columns = scale_cols)
        # y = pd.concat([self.out[['Country', 'Date', 'Curr']], transformed], axis = 1)
        # # y = pd.concat([self.out[['Country', 'Date', 'Curr']], pd.DataFrame(scaler.fit_transform(self.out[scale_cols]), columns = scale_cols)], axis = 1)
        # # self.out = self.out[[x for x in not_scale_list if x in self.out.columns]].merge(y, on = ['Country', 'Date', 'Curr'])
        # merge_col_list = [x for x in not_scale_list if x in self.out.columns]
        # print('merge cols are:')
        # print(merge_col_list)
        # self.out = self.out[merge_col_list]
        # print('after filtering, out shape is')
        # print(self.out.shape)
        # print('y shape is')
        # print(y.shape)

        # self.out = pd.concat([self.out, y], axis = 1)
        # print('after concatenation, out shape is')
        # print(self.out.shape)
        outname = self.dir + '/Tim_Code/Output_Data/cmc_scaled_regdata_' + self.pull_date + '.csv'
        self.out.to_csv(outname)
        print('Regression data written out')



    def get_all_countries_data(self):

        '''
        This function combines the data for all countries.
        '''

        print('Getting country data.')
        # [self.get_country_data_all(ctry) for ctry in tqdm(['AUSTRIA'])]
        # Parallel(n_jobs = self.cores)(delayed(self.get_country_data_all)(ctry) for ctry in tqdm(['AUSTRIA', 'BELGIUM']))
        Parallel(n_jobs = self.cores)(delayed(self.get_country_data_all)(ctry) for ctry in tqdm(self.s['Country (Full Name)'].unique()))
        self.out = pd.concat(Parallel(n_jobs = self.cores)(delayed(pd.read_csv)(self.dir + '/Tim_Code/Output_Data/' + f, index_col = 0) for f in listdir(self.dir + '/Tim_Code/Output_Data') if f.endswith('cmc_regression_data_' + self.pull_date + '.csv')), ignore_index = True)
        print('out shape is')
        print(self.out.shape)
        print(self.out.head())
        print([x for x in self.out.columns])
        # Clean up the column arrangements.
        self.filter_data()



    def main(self):
        '''
        This runs everything.
        '''
        self.load_election_data()
        self.load_issue_data()
        self.get_currencies()
        # self.get_all_countries_data()



if __name__ == "__main__":
    debt = Debt_Issues(pull_date = '2020-10-26')
    debt.main()

ctry = 'ARGENTINA'
curr = 'USD'
date = pd.to_datetime('1998-01-01')
ctry_df = debt.ctry_month_curr[debt.ctry_month_curr['Country (Full Name)'] == ctry].sort_values(['Date'])
ctry_df['Date'] = pd.to_datetime(ctry_df['Date'])
ctry_df[ctry_df['Date'] == '1998-01-01']

ctry_df = debt.s[debt.s['Country (Full Name)'] == ctry]
ctry_df[(ctry_df['Curr'] == curr) & (ctry_df['Issue Date'].dt.strftime('%Y-%m') == date.strftime('%Y-%m')) ]['Amount Issued']#.sum(skipna = True)
u = debt.s[['Country (Full Name)', 'Issue Date', 'Curr', 'Amount Issued']].drop_duplicates()
u = u[u['Issue Date'] >= '1998-01-01']
u = u[u['Country (Full Name)'].isin(['POLAND', 'INDONESIA', 'DOMINICAN REPB.', 'COLOMBIA', 'MOROCCO',
       'LITHUANIA', 'BRAZIL', 'MEXICO', 'CROATIA', 'COSTA RICA',
       'ECUADOR', 'CHILE', 'PANAMA', 'EL SALVADOR', 'PARAGUAY',
       'KAZAKHSTAN', 'PERU', 'SRI LANKA', 'UKRAINE', 'CYPRUS', 'KENYA',
       'MONGOLIA', 'PHILIPPINES', 'URUGUAY', 'NIGERIA', 'ARGENTINA'])]
u


temp = Parallel(n_jobs = self.cores)(delayed(self.get_date_vars)(ctry_df.iloc[x,]) for x in tqdm(range(ctry_df.shape[0])))


g = debt.ctry_month_curr
g[(g['Country (Full Name)'] == 'ARGENTINA') & (g['Date'] == '1998-01-01')]
debt.ctry_month_curr.head()
