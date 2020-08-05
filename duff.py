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
#import modin.pandas as pd_modin
import matplotlib.pyplot as plt
from datetime import date, timedelta
# from itertools import permutations, chain

path = '/Users/Dan/Desktop/sumpro/Ticker_AAPL.txt'
stock_files = glob.glob(path)

def get_txt(files):
    symbols = pd.concat([pd.read_csv(symbols, dtype=str, error_bad_lines=False, delimiter='\t') for symbols in files], axis=0)
    df = symbols.drop_duplicates(keep='first').reset_index()
    return df

tickers = ((get_txt(stock_files))['Symbol']).sort_values().reset_index(drop=True)

stocks_start = datetime.datetime(2020, 5, 1)
stocks_end = datetime.datetime(2020, 7, 31)

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

'''
        
def results(portfolio):
    profit = ((portfolio.free_money + portfolio.utilized_money) - portolfio.original_balance)
    pct_gain = ((((portfolio.free_money + portfolio.utilized_money) / portfolio.original_balance) - 1) * 100)

    print("Holdings: ", portfolio.holdings)
    print("Profit: $" + str(profit))
    print("Percent Gain: " + str(pct_gain) + "%")

'''
        
folio_1 = portfolio(50000)

def data_and_trader(portfolio, tickers, startdate, enddate):
    for ticker in tickers:
        portfolio.holdings[ticker] = 0
        def data(ticker):
            try:                
                current_ticker = (yf.download(ticker, start=startdate, end=enddate)).reset_index(level=['Date'])
                current_ticker['10 Day Moving Average'] = current_ticker.iloc[:,5].rolling(window=10).mean()
                current_ticker['10 DAY MAC'] = current_ticker.iloc[:,7].diff()
                
                # Finviz Scraper
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
                        
                    print(current_ticker.iloc[-1][5])
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
                    
                    print(ticker)
                    print(current_ticker)

                except (IndexError):
                    pass
                
                '''
                
                # Plot Data
                ax = plt.gca()
                current_ticker.plot(kind='line', x='Date', y='7_day_ma', ax=ax)
                plt.show()
                
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
                
                return current_ticker

            except (ValueError, KeyError):
                pass
            time.sleep(1)
    datas = map(data, tickers)
    return(pd.concat(datas, keys=tickers, names=['Ticker']).reset_index())
    #return(pd.concat(keys=tickers, names=['Ticker']).reset_index())
    
start_time = time()

stock_data = data_and_trader(folio_1, tickers, stocks_start, stocks_end)#.reset_index()

# Optional CSV Storage
stock_data.to_csv('/Users/Dan/Desktop/sumpro/stock_data.csv', index=0)

'''

print("Number of processors: ", mp.cpu_count())
os.environ["MODIN_ENGINE"] = "dask"
import modin.pandas as pd_modin

def read_data(chunksize):
    chunksize = chunksize
    sd = pd_modin.read_csv(..., chunksize=chunksize, iterator=True)
    stock_data = pd_modin.concat(sd, ignore_index=True)
    return stock_data

stock_data = read_data(100000)

'''

'''

def real_data():
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

end_time = time()
elapsed_time = float(end_time - start_time)

print("Data Columns: " + str(list(stock_data.columns)))
print("time elapsed (in seconds): " + str(round(elapsed_time, 2)))
print("time elapsed (in minutes): " + str(round(elapsed_time / 60.0, 3)))
print('# of Rows = ' + str(len(stock_data)))
print('Min Date = ' + str(min(stock_data['Date'])))
print('Max Date = ' + str(max(stock_data['Date'])))
print('# of Tickers = ' + str(len(stock_data['Ticker'].unique())))
