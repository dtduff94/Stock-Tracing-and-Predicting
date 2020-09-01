# Imports
import os
import math
import glob
import timeit
import requests
import datetime
import itertools
import numpy as np
import pandas as pd
import yfinance as yf
import multiprocessing as mp
from time import time, sleep
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
from datetime import date, timedelta
from pandas_datareader import data as pdr

# Start Timer for Checking Speed of Program
start_time = time()

# If your code uses pandas_datareader and you want to download data faster, you can “hijack” pandas_datareader.data.get_data_yahoo() method to use yfinance while making sure the returned data is in the same format as pandas_datareader’s get_data_yahoo().
yf.pdr_override()

# Path of .txt file with stock tickers to get data for
path = '/Users/Dan/Desktop/sumpro/Ticker_AAPL.txt'
stock_files = glob.glob(path)

# Get tickers out of .txt file
def get_txt(files):
    symbols = pd.concat([pd.read_csv(symbols, dtype=str, error_bad_lines=False, delimiter='\t') for symbols in files], axis=0)
    df = symbols.drop_duplicates(keep='first').reset_index()
    return df

# Generates list of tickers
tickers = ((get_txt(stock_files))['Symbol']).sort_values()

# Start and end dates for data.
stocks_start = datetime.datetime(2020, 5, 1)
stocks_end = datetime.datetime(2020, 8, 27)

# Portfolio Class
class portfolio(object):
    # Dictionary for holdings, key is ticker, value is shares, should add a total (OLD)
    # money made on ticker, pct gain, possibly update price if possible as days go by
    # {ticker-string-key : [shares owned-int, [list of purchase dates], [list of sell dates],
    # money made on ticker-int, pct gain on ticker-int, (maybe price? - if I can update) - price,
    # DUFF score]-list of attributes-value}
    holdings = {}
    original_balance = 0.0
    free_money = 0.0
    utilized_money = 0.0
    profit = 0.0
    pct_gain = 0.0

    def __init__(self, money):
        self.original_balance = money
        self.free_money = money
        
    def buy(self, ticker, shares, price):
        if self.free_money < (shares * price):
            possible_shares = math.floor((self.free_money / price))

            print("Action: BUY")
            print("Funds: INSUFFICIENT - You can afford " + str(possible_shares) + " shares")
            
            if (not ticker in self.holdings) or (self.holdings.get(ticker) == 0): 
                self.holdings[ticker] = possible_shares
                self.free_money = self.free_money - (possible_shares * price)
                self.utilized_money = self.utilized_money + (possible_shares * price)
            else:
                self.holdings[ticker] = self.holdings.get(ticker) + possible_shares
                self.free_money = self.free_money - (possible_shares * price)
                self.utilized_money = self.utilized_money + (possible_shares * price)
        else:
            print("Action: BUY")
            print("Funds: AVAILABLE")
            if not ticker in self.holdings: 
                self.holdings[ticker] = shares 
                self.free_money = self.free_money - (shares * price)
                self.utilized_money = self.utilized_money + (shares * price)
            else:
                self.holdings[ticker] = self.holdings.get(ticker) + shares
                self.free_money = self.free_money - (shares * price)
                self.utilized_money = self.utilized_money + (shares * price)
                
    def sell(self, ticker, shares, price):
        if (not ticker in self.holdings) or (self.holdings.get(ticker) == 0):
            print("Action: SELL")
            print("You do not have any of this equity to sell.") 
            return
        if self.holdings.get(ticker) < shares:
            print("Action: SELL")
            print("You do not have enough of this equity to sell.  You have " + self.holdings.get(ticker) + " shares.")
            self.free_money = self.free_money + (self.holdings.get(ticker) * price)
            self.utilized_money = self.utilized_money - (self.holdings.get(ticker) * price)
            if self.utilized_money < 0:
                self.utilized_money = 0
        else:
            print("Action: SELL")
            self.free_money = self.free_money + (self.holdings.get(ticker) * price)
            self.utilized_money = self.utilized_money - (self.holdings.get(ticker) * price)
            if self.utilized_money < 0:
                self.utilized_money = 0
            self.holdings[ticker] = self.holdings.get(ticker) - shares

    def results(self):
        self.profit = ((self.free_money + self.utilized_money) - self.original_balance)
        self.pct_gain = ((((self.free_money + self.utilized_money) / self.original_balance) - 1) * 100)

        print("Holdings: ", self.holdings)
        print("Profit: $" + str(self.profit))
        print("Percent Gain: " + str(self.pct_gain) + "%")

# Creating Proper Dataframe
df_list = []
separate_df_dict = {}

for ticker in tickers:
    current_ticker_date_index = pdr.get_data_yahoo(ticker, start=stocks_start, end=stocks_end) #columns 1-7
    current_ticker = current_ticker_date_index.reset_index()
    current_ticker.insert(loc=0, column='Ticker', value=ticker)

    '''
    
    current_ticker['10 Day Moving Average'] = current_ticker['Close'].rolling(window=10).mean() #column 8
    current_ticker['30 Day Moving Average'] = current_ticker['Close'].rolling(window=30).mean() #column 9
    current_ticker['50 Day Moving Average'] = current_ticker['Close'].rolling(window=50).mean() #column 10
    current_ticker['100 Day Moving Average'] = current_ticker['Close'].rolling(window=100).mean() #column 11
    current_ticker['200 Day Moving Average'] = current_ticker['Close'].rolling(window=200).mean() #column 12

    '''
    
    # EMA's, MACD, Bollinger Bands
    for i in range(len(current_ticker)):
        current_ticker['12 Day EMA'] = current_ticker['Close'].ewm(span=12).mean() #column 13
        current_ticker['26 Day EMA'] = current_ticker['Close'].ewm(span=26).mean() #column 14
        current_ticker['MACD'] = current_ticker['12 Day EMA'] - current_ticker['26 Day EMA'] #column 15

        current_ticker['30 Day MA'] = current_ticker['Adj Close'].rolling(window=20).mean() #column 16

        # set .std(ddof=0) for population std instead of sample
        current_ticker['30 Day STD'] = current_ticker['Adj Close'].rolling(window=20).std() #column 17
    
        current_ticker['Upper Band'] = current_ticker['30 Day MA'] + (current_ticker['30 Day STD'] * 2) #column 18
        current_ticker['Lower Band'] = current_ticker['30 Day MA'] - (current_ticker['30 Day STD'] * 2) #column 19
        current_ticker['Band Percentage'] = current_ticker['Close'] / current_ticker['30 Day MA'] #column 20
        current_ticker['Lower Band Percentage'] = current_ticker['Close'] / current_ticker['Lower Band'] #column 21
        current_ticker['Upper Band Percentage'] = current_ticker['Close'] / current_ticker['Upper Band'] #column 22
        current_ticker['Close to Lower Diff'] = current_ticker['Close'] - current_ticker['Lower Band'] #column 23
        current_ticker['Mid to Lower Diff'] = current_ticker['30 Day MA'] - current_ticker['Lower Band'] #column 24
        current_ticker['Upper to Lower Diff'] = current_ticker['Upper Band'] - current_ticker['Lower Band'] #column 25
        current_ticker['Lower Base'] = 0.00000000000000001 #column 26
        
    current_ticker['9 Day MACD EMA'] = current_ticker['MACD'].ewm(span=9).mean() #column 27
    for i in range(len(current_ticker)):
        current_ticker['MACD Convergence/Divergence'] = current_ticker['MACD'] - current_ticker['9 Day MACD EMA'] #column 28

    current_ticker['Divergence Change'] = current_ticker['MACD Convergence/Divergence'].diff() #column 29

    '''
    
    # Yahoo Finance Website Additional Data Scraping
    url_to_scrape = 'https://finance.yahoo.com/quote/' + ticker + '?p=' + ticker + '&.tsrc=fin-srch'
    
    #Load html's plain data into a variable
    plain_html_text = requests.get(url_to_scrape)
            
    #parse the data
    soup = BeautifulSoup(plain_html_text.text, "html.parser")

    try:
        #Get all data associated with this class
        stock_table = soup.find("table", {"class": "W(100%) M(0) Bdcl(c)"})
        rows = stock_table.select("td")
        
        marketcap = str((rows[1].findAll("span")[0].text).replace(".", ""))
        if marketcap[-1] == "T":
            marketcap = marketcap.replace("T", "000000000")
        elif marketcap[-1] == "B":
            marketcap = marketcap.replace("B", "000000")
        elif marketcap[-1] == "M":
            marketcap = marketcap.replace("M", "000")
            
        num_of_shares = (int(marketcap) / (current_ticker.iloc[-1][5]))
        current_ticker['Market Cap'] = (current_ticker['Adj Close'] * num_of_shares) #column 30
    
        beta_five_year_monthly = rows[3].findAll("span")[0].text
        current_ticker['Beta Five Year Monthly'] = beta_five_year_monthly
        
        pe_ratio_ttm = rows[5].findAll("span")[0].text
        current_ticker['PE Ratio (TTM)'] = pe_ratio_ttm
        
        eps_ttm = rows[7].findAll("span")[0].text
        current_ticker['EPS (TTM)'] = eps_ttm
        
        earnings_date = rows[9].findAll("span")[0].text
        current_ticker['Earnings Date'] = earnings_date
        
        one_year_target_est = rows[15].findAll("span")[0].text
        current_ticker['One Year Target Estimate'] = one_year_target_est

    except(IndexError, ValueError):
        pass
    
    '''
    
    df_list.append(current_ticker)
    separate_df_dict[ticker] = current_ticker
    
all_tickers = pd.concat(df_list)
        
# Sorts DataFrame by Date Column and turns that column into the index
all_tickers['Date'] = pd.to_datetime(all_tickers['Date'])

# all_tickers.index = all_tickers['Date']
# del all_tickers['Date']

all_tickers = all_tickers.sort_values(by = ['Date', 'Ticker'], ascending = [True, True])
#End of Creating Proper DataFrame

'''

# Plot Data
ax = plt.gca()
(separate_df_dict['AAPL']).plot(kind='line', x='Date', y='Divergence Change', ax=ax)
all_tickers.plot(kind='line', x='Date', y='MACD Derivative', ax=ax)
plt.show()

'''

#Portfolio Creation and Trading

folio_1 = portfolio(50000)

# Stock Dates
stock_dates = all_tickers['Date'].unique()

#print(stock_dates)

def trader(portfolio, dataframe, df_dict):
    for date in stock_dates:
        print(((all_tickers.loc[(all_tickers['Date'] == date) & (all_tickers['Lower Band Percentage'] > 1)]).nsmallest(10, 'Lower Band Percentage')).nsmallest(5, 'Upper Band Percentage'))

'''

        for i in range(len(current_ticker)):
                    if current_ticker.iloc[i - 1][8] > 0 and current_ticker.iloc[i - 2][8] < 0:
                        print("BUY DATA:")
                        print("Date: ", current_ticker.iloc[i][0])
                        print("Purchase Price: ", current_ticker.iloc[i][1])
                        print("7 Day Moving Average Change: ", current_ticker.iloc[i][8])
                        print("Total Money Available Before Purchase: ", str(portfolio.free_money))
                        print("Total Money Unavailable Before Purchase: ", str(portfolio.utilized_money))
                        print("Shares Before Buying: ", str(portfolio.holdings.get(ticker)))
                        print("Holdings Before Sale: ", portfolio.holdings)
                        print("vvvvvvvvvvvvvvvvvvvvvvvvvvvvv")
                        portfolio.buy(ticker, math.floor((portfolio.free_money)/(current_ticker.iloc[i][1])), (current_ticker.iloc[i][1]))
                        print("Total Money Available After Transaction: ", str(portfolio.free_money))
                        print("Total Money Unavailable After Transaction: ", str(portfolio.utilized_money))
                        print("Shares After Buying: ", str(portfolio.holdings.get(ticker)))
                        print("Holdings After Buying: ", portfolio.holdings)
                        print("Natural Profit: ", str(((((current_ticker.iloc[i][5]) / (current_ticker.iloc[0][1])) - 1) * (portfolio.original_balance))))
                        print("Natural Percent Gain: ", str((((current_ticker.iloc[i][5]) / (current_ticker.iloc[0][1])) - 1) * 100))
                        print("---------------------------------------------------------------------")
                    
                    if current_ticker.iloc[i - 1][8] < 0 and current_ticker.iloc[i - 2][8] > 0:
                        print("SELL DATA:")
                        print("Date: ", str(current_ticker.iloc[i][0]))
                        print("Selling Price: ", str(current_ticker.iloc[i][1]))
                        print("7 Day Moving Average Change: ", str(current_ticker.iloc[i][8]))
                        print("Total Money Available Before Sale: ", str(portfolio.free_money))
                        print("Total Money Unavailable Before Sale: ", str(portfolio.utilized_money))
                        print("Shares Before Sale: ", str(portfolio.holdings.get(ticker)))
                        print("Holdings Before Sale: ", portfolio.holdings)
                        print("vvvvvvvvvvvvvvvvvvvvvvvvvvvvv")
                        portfolio.sell(ticker, portfolio.holdings.get(ticker), (current_ticker.iloc[i][1]))
                        print("Total Money Available After Sale: ", str(portfolio.free_money))
                        print("Total Money Unavailable After Sale: ", str(portfolio.utilized_money))  
                        print("Shares After Sale: ", str(portfolio.holdings.get(ticker)))
                        print("Holdings After Sale: ", portfolio.holdings)
                        print("Natural Profit: ", str(((((current_ticker.iloc[i][5]) / (current_ticker.iloc[0][1])) - 1) * (portfolio.original_balance))))
                        print("Natural Percent Gain: ", str((((current_ticker.iloc[i][5]) / (current_ticker.iloc[0][1])) - 1) * 100))
                        print("---------------------------------------------------------------------")
                        
                portfolio.results()

'''
                
        
trader(folio_1, all_tickers, separate_df_dict)


#lowest on low band percent and lowest on high band percent, hold big tech till overbought (try this way, also try with strictly following the bands), hold others till they need to be sold for something else and have worse percentage data, use broader economic data to decide when to be majority cash or equity - MACD on indexes, buy 3x weighted indexes after large sell-offs (>15-20%)

#(dataframe[dataframe['Band Percentage'].min()]) &
#print(((all_tickers.loc[(all_tickers['Date'] == '2020-05-05') & (all_tickers['Lower Band Percentage'] > 1)]).nsmallest(10, 'Lower Band Percentage')).nsmallest(5, 'Upper Band Percentage'))
#print((all_tickers.loc[(all_tickers['Date'] == '2020-08-25') & (all_tickers['Ticker'] == 'TSLA')])) #['MACD'].values[0])
#print((all_tickers.loc[(all_tickers['Date'] == '2020-08-25')])['Lower Band Percentage'].min())

'''

def trader1(portfolio, dataframe, df_dict):
    #for i in range(len(dataframe)):
    print(dataframe[dataframe['Market Cap'] > 2000000000000]) # and (df_dict[dataframe['Ticker']])['Date'] == dataframe['Date']
        #print(str(len(dataframe['Ticker'].unique())))

trader1(folio_1, all_tickers, separate_df_dict)

'''

# Test Print out just AAPL's Date Frame
#print(separate_df_dict['AAPL'])

#Testing and Printing

print(all_tickers)

end_time = time()
elapsed_time = float(end_time - start_time)

print("Data Columns: " + str(list(all_tickers.columns)))
print("time elapsed (in seconds): " + str(round(elapsed_time, 2)))
print("time elapsed (in minutes): " + str(round(elapsed_time / 60.0, 3)))
print('# of Rows = ' + str(len(all_tickers)))
print('Min Date = ' + str(all_tickers['Date'].min()))
print('Max Date = ' + str(all_tickers['Date'].max()))
print('# of Tickers = ' + str(len(all_tickers['Ticker'].unique())))

