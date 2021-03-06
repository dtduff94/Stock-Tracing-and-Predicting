import datetime
from time import time, sleep
import os
import pandas as pd
import yfinance as yf
import numpy as np
import timeit
import glob
import itertools
from itertools import permutations, chain
# import fix_yahoo_finance as yf
# from datetime import datetime
# import matplotlib
# matplotlib.use('TkAgg')
# import matplotlib.pyplot as plt
# DUFF ;)
import requests
# import MySQLdb
import mysql.connector

#SQL connection data to connect and save the data in
HOST = "localhost"
USERNAME = "scraping_sample_user1"
PASSWORD = "Bullseye18!"
DATABASE = "scraping_sample1"

db = mysql.connector.connect(
    host = "localhost",
    user = "scraping_sample_user1",
    password = "Bullseye18!",
    database = "scraping_sample1"
)

# prepare a cursor object using cursor() method
# mycursor = mydb.cursor()
mycursor = db.cursor()

path = '/Users/Dan/Desktop/sumpro/Ticker_AAPL.txt'
stock_files = glob.glob(path)

def get_txt(files):
    symbols = pd.concat([pd.read_csv(symbols, dtype=str, error_bad_lines=False, delimiter='\t') for symbols in files], axis=0)
    df = symbols.drop_duplicates(keep='first').reset_index()
    return df

stocks_raw = get_txt(stock_files)
tickers = stocks_raw['Symbol']
tickers = tickers.sort_values().reset_index(drop=True)

stocks_start = datetime.datetime(1962, 1, 1)
stocks_end = datetime.datetime(2020, 8, 7)

def get_stock_data(tickers, startdate, enddate):
    def data(ticker):
        for tickers in ticker:
            try:                
                current_ticker = (yf.download(ticker, start=startdate, end=enddate)).reset_index(level=['Date'])
                #print(current_ticker)           

                try:
                    # SQL statement
                    sql = "CREATE TABLE IF NOT EXISTS " + ticker + " (ticker VARCHAR(10), date TIMESTAMP PRIMARY KEY, open DECIMAL(7, 3), high DECIMAL(7,3), low DECIMAL(7, 3), close DECIMAL(7, 3), adj_close DECIMAL(7, 3), volume INT(20));"

                    # Works in creating new table -- deleted other MA's and program failed to show being finished but it worked (IMPORTANT)
                    #sql = "CREATE TABLE " + ticker + "_ma AS (SELECT a.date, a.close, Round((SELECT SUM(b.close) / COUNT(b.close) FROM " + ticker + " AS b WHERE DATEDIFF(a.date, b.date) BETWEEN 0 AND 4), 2) AS '5_day_ma' FROM " + ticker + " AS a ORDER BY a.date);"
                    
                    # print(sql)
                    
                    # Execute the SQL command
                    mycursor.execute(sql)

                    # Commit your changes in the database
                    db.commit()
                    
                    # mycursor.execute(sql_one)

                    # Commit your changes in the database
                    # db.commit()
                    
                    for i in range(19000): #19000
                        try:
                            #print(i)
                            sql_two = "INSERT INTO " + ticker + "(ticker, date, open, high, low, close, adj_close, volume) VALUES('" + ticker + "', '" + str(current_ticker.iloc[i][0]) + "', " + str(current_ticker.iloc[i][1]) + ", " + str(current_ticker.iloc[i][2]) + ", " + str(current_ticker.iloc[i][3]) + ", " + str(current_ticker.iloc[i][4]) + ", " + str(current_ticker.iloc[i][5]) + ", " + str(current_ticker.iloc[i][6]) + ");"
                            #print(sql_two)
                            
                            # Execute the SQL command
                            mycursor.execute(sql_two)
                            # Execute your changes in the database
                            db.commit()
                        except:
                            # Rollback changes if there is an erroneous operation
                            db.rollback()
                            pass
                    
                except:
                     # Rollback changes if there is an erroneous operation
                    db.rollback()
                
                return (yf.download(ticker, start=startdate, end=enddate))
            except (ValueError, KeyError):
                pass
            time.sleep(2)
    datas = map(data, tickers)
    return(pd.concat(datas, keys=tickers, names=['Ticker', 'Date']).reset_index())

#
t1 = tickers

start_time = time()

stock_data = get_stock_data(t1, stocks_start, stocks_end)#.reset_index()

"""
Create sqlite3 database for data storage?
"""
# Optional CSV Storage
# stock_data.to_csv('/Users/Dan/Desktop/sumpro/stock_data.csv', index=0)
#

end_time = time()
elapsed_time = float(end_time - start_time)

print("Data taken: " + str(list(stock_data.columns)))
print("time elapsed (in seconds): " + str(round(elapsed_time, 2)))
print("time elapsed (in minutes): " + str(round(elapsed_time / 60.0, 3)))
print('# of Rows = ' + str(len(stock_data)))
print('Min Date = ' + str(min(stock_data['Date'])))
print('Max Date = ' + str(max(stock_data['Date'])))
print('# of Tickers = ' + str(len(stock_data['Ticker'].unique())))
