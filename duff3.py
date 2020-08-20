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

# If your code uses pandas_datareader and you want to download data faster, you can “hijack” pandas_datareader.data.get_data_yahoo() method to use yfinance while making sure the returned data is in the same format as pandas_datareader’s get_data_yahoo().
yf.pdr_override()

# Path of .txt file with stock tickers to get data for.
path = '/Users/Dan/Desktop/sumpro/Ticker_AAPL.txt'
stock_files = glob.glob(path)

# Get tickers out of .txt file.
def get_txt(files):
    symbols = pd.concat([pd.read_csv(symbols, dtype=str, error_bad_lines=False, delimiter='\t') for symbols in files], axis=0)
    df = symbols.drop_duplicates(keep='first').reset_index()
    return df

# Generates list of tickers
tickers = ((get_txt(stock_files))['Symbol']).sort_values()

# Start and end dates for data.
stocks_start = datetime.datetime(2020, 5, 1)
stocks_end = datetime.datetime(2020, 8, 20)

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
    current_ticker_date_index = pdr.get_data_yahoo(ticker, start=stocks_start, end=stocks_end)
    current_ticker = current_ticker_date_index.reset_index()
    current_ticker.insert(loc=0, column='Ticker', value=ticker)
    
    current_ticker['10 Day Moving Average'] = current_ticker.iloc[:,5].rolling(window=10).mean()
    current_ticker['10 DAY MAC'] = current_ticker.iloc[:,7].diff()
    
    # Yahoo Finance Scraper
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
        current_ticker['Market Cap'] = (current_ticker['Adj Close'] * num_of_shares)
        
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

    df_list.append(current_ticker)
    separate_df_dict[ticker] = current_ticker
    
all_tickers = pd.concat(df_list)

# Sorts DataFrame by Date Column and turns that column into the index
all_tickers['Date'] = pd.to_datetime(all_tickers['Date'])
all_tickers.index = all_tickers['Date']
del all_tickers['Date']
all_tickers = all_tickers.sort_values(by = ['Date', 'Ticker'], ascending = [True, True])
# End of Creating Proper DataFrame

# Print DataFrame
print(all_tickers)

# Test Print out just AAPL's Date Frame
# print(separate_df_dict['AAPL'])
