#!/usr/bin/env python2 <- suppose to be python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 24 11:13:32 2017

@author: toby

1. Thinking about combining everything needed into this file, without import functions from other parts <- make it easier
to fix and update.

2. involve dynamodb write into jobs.


Problems:
    1. when does slickcharts update their sp500 data daily? -- shall be done before the market opens.
    2. does googlesheet update automatically? or need extra script? -- automatically, about 2mins / time
    

Update 5-19-18: 
    changed from google to yahoo for function H2_read_option_ws, and also, I dont think we need H1 function anymore.    
"""

import schedule
import time
#import boto3
import pandas as pd
import datetime
import decimal
import os
from bs4 import BeautifulSoup
import urllib3
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pandas_datareader.data import Options
import traceback
import random as rd

def D1_crawlsp(dy):
    """
    input: directory to save readed file
    output: a list of symbols, + write symbol & weight info in files.
    Completed at Jul 26
    """
    print("D1_crawlsp running....")
    http = urllib3.PoolManager()
    url = "http://slickcharts.com/sp500"
    r = http.request('GET',url)
    soup = BeautifulSoup(r.data,'lxml')
    weight = soup.find_all("td")
    symbol_store = []
    weight_store = []
    for i in range(len(weight)):
        tmp = weight[i]
        txt = str(tmp)
        if u'input name="symbol" type="hidden"' in txt:
            loc1 = txt.find('value=')
            loc2 = txt.find('<input type=')
            txt_tmp = txt[loc1:loc2]
            symbol = txt_tmp.split('"')[1]
            symbol_store.append(symbol)
            tmp2 = weight[i+1]
            txt2 = str(tmp2)
            wf_tmp = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", txt2)[0]#weight factor
            wf = float(wf_tmp)
            weight_store.append(wf)
            #cool, seems working great
    df = pd.DataFrame({
            'symbol':symbol_store,
            'weight':weight_store
            })
    path = dy + "/symbolweight.txt"
    df.to_csv(path, index=None, sep=' ', mode='w')
#    print("DONE")
    path2 = dy + "/sp500_symbols.txt"
    df2 = df['symbol']
    df2.to_csv(path2, header = None, index=None, sep=' ', mode='w')
#    print("DONE2")
    print(symbol_store)
    print("D1_crawlsp finished. \n")
    return symbol_store

def D2_update_quotes(symbol_store):
    '''
    input: a list of symbols (updated from D1_crawlsp)
    output: update the google sheet accordingly, for further read.
    completed at Jul 26
    '''
    # read option
    print("D2 google sheets update running... ")
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://spreadsheets.google.com/feeds']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)
     
    # Find a workbook by name and open the first sheet
    # Make sure you use the right name here.
    sheet = client.open("GoogleFinanceAPI").sheet1
    print("length of symbol_store :  ", len(symbol_store))
    for s in range(len(symbol_store)):
        if creds.access_token_expired:
            client.login()  # refreshes the token
        sheet.update_cell(s+2,1,symbol_store[s])
        sheet.update_cell(s+2,2,"=GOOGLEFINANCE(A"+str(s+2)+", $B$1)")
        sheet.update_cell(s+2,3,"=GOOGLEFINANCE(A"+str(s+2)+", $C$1)")
        sheet.update_cell(s+2,4,"=GOOGLEFINANCE(A"+str(s+2)+", $D$1)")
        sheet.update_cell(s+2,4,"=GOOGLEFINANCE(A"+str(s+2)+", $E$1)")
        print("updating google cell..."+str(s+1),symbol_store[s])
    print("D2 google sheets update finished. \n")   
    return True     

def formulate_directory(dy):
    '''
    well...the goal for this to allow autoupdate of directory.
    but,..one needs to be careful, since the input can only take oridinary dys:
    input: a directory with file extension to update
    output: create a new formated directory and return
    
    Example:
        input:
        path = '.../GoolgeAPI_sp500.txt'
        
        output:
        return = '.../GoolgeAPI_sp500_0.txt'
    
        but...if you use that output as the new input, you will get:
            '.../GoolgeAPI_sp500_0_0.txt'
    '''
    [path, file_exten] = dy.rsplit('.', 1)
    string = path+"_%s."+file_exten
    i = 0
    while os.path.exists(string % i):
        i += 1
    dy_new = string.replace("%s",str(i))
    return dy_new

def H1_read_quotes_ws(symbol_store, dy):
    '''
    read quotes data from google finances on google sheet.
    input: 1. a list of symbols, 2. the directory to save
    output: 1. a dataframe with all sp500 stocks 2. index value of sp500, 3. both of 1&2 written into files
    '''
    # read option
    print("H1 google sheets quotes data reading...")
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://spreadsheets.google.com/feeds']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)
     
    # Find a workbook by name and open the first sheet
    # Make sure you use the right name here.
    sheet = client.open("GoogleFinanceAPI").sheet1
    if creds.access_token_expired:
        client.login()  # refreshes the token    
    print("number of symbols :  " + str(len(symbol_store)))
    # Extract and print all of the values
    list_of_hashes = sheet.get_all_records()
    dfdf = pd.DataFrame(list_of_hashes)
    
    client = gspread.authorize(creds)
    sheet_index = client.open("GoogleFinanceAPI_Index").sheet1
    if creds.access_token_expired:
        client.login()  # refreshes the token    
    spindex = sheet_index.get_all_records()
    current = spindex[0]['time']
    path = dy + '/GoolgeAPI_sp500.txt'
    path_f = formulate_directory(path)
    dfdf['time'] = current
    dfdf.to_csv(path_f,sep=',', header=None, index=None,mode ='w')

    path2 = dy + '/GoolgeAPI_spindex.txt'
    path2_f = formulate_directory(path2)
    f = open(path2_f,'w')
    for value in spindex:
        print(value)
        f.write(str(value))
    f.close()
    print("H1 finish google sheets quotes reading. \n\n")
    return [dfdf,spindex]

def H1_read_quotes_wts(dy):
    indexfile = dy + "/sp500_symbols.txt"
    indexlist = [] #set 1, len = 506
    with open(indexfile,'r') as f:
        for lines in f:
            indexlist.append(lines.replace("\n",""))
#    return indexlist #ok..it works~
    return H1_read_quotes_ws(indexlist, dy)


def H2_read_option_ws(symbol_store, dy):
    """
    This function stands for hourly job 2, read option with symbol.
    Input here: 
        -symbol_store = a list of symbols that shall be updated daily
        -dy = directory in string about where to find and store data.
    
    5-19-18: changed from google to yahoo for function H2_read_option_ws
    and also, I dont think we need H1 function anymore.
    """
    
    print("H2 google finance options info reading...")
    errorlist = []
    spfile = dy+"/sp500.json"
    spfile_f = formulate_directory(spfile)
    index = 1
    for symbol in symbol_store:
        try:
            data = Options(symbol, 'yahoo') # read the option data from google finance
            exp_list = [data.expiry_dates[0]]
            print('1',symbol, max(exp_list))
            for i in range(10):
                data = Options(symbol, 'yahoo') # read the option data from google finance
                exp_list.append(data.expiry_dates[0])
            
            df = data.get_options_data(expiry=max(exp_list))
            row_num = df.shape[0]
            for j in range(10):
                df_tmp = data.get_options_data(expiry=max(exp_list))
                if df_tmp.shape[0] > row_num:
                    print("find larger df for %s, row num change %d -> %d" %(symbol, row_num, df_tmp.shape[0]))
                    df = df_tmp.copy()
                    row_num = df_tmp.shape[0]
            df.to_csv(spfile_f,sep=',',mode ='a')
            print(index, " Done for :", symbol,"\n")
        except TypeError:
            traceback.print_exc()
            errorlist.append(symbol)
            continue #shall this be continue?
        except KeyError:
            traceback.print_exc()
            print('2',symbol, max(exp_list))
            print("error with stock:  ",symbol)
            errorlist.append(symbol)
            continue
        except Exception:
            traceback.print_exc()
            print('3',symbol, max(exp_list))
            print("error with stock:  ",symbol)
            errorlist.append(symbol)
            continue  
        index += 1   
        factor = rd.random() * 15
        time.sleep(0.1 * factor)
    path_error = dy + "/errorlist.txt"
    f = open(path_error,'w') #shall use 'a' for append
    for elements in errorlist:
        f.write(str(elements))
    f.close()
    print("H2 options info reading finished.")
    print("number of symbols with error: " + str(len(errorlist)) + "\n\n")

def H2_read_option_wts(dy):
    indexfile = dy + "/sp500_symbols.txt"
    indexlist = [] #set 1, len = 506
    with open(indexfile,'r') as f:
        for lines in f:
            indexlist.append(lines.replace("\n",""))
    return H2_read_option_ws(indexlist, dy)    
    
def H3_read_bankrate(dy):
    '''
    read the bankrates data from bllomberg:
        https://www.bloomberg.com/markets/rates-bonds/government-bonds/us
    and save the result into the txt file [!!!name format depends on daily/hourly]
    '''
    print("H3 bank-rate read running.")
    http = urllib3.PoolManager()
    url = "https://www.bloomberg.com/markets/rates-bonds/government-bonds/us"
    r = http.request('GET',url)
    soup = BeautifulSoup(r.data,'lxml')
    info = soup.find_all("td")

    for i in range(len(info)):
        txt = str(info[i])
        if "GB3:GOV" in txt:
            coupon_txt = str(info[i+1])
            cou = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", coupon_txt)[0]
            price_txt = str(info[i+2]) 
            pri = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", price_txt)[-1]
            yield_txt = str(info[i+3]) 
            yie = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*",yield_txt)[0]
            month_txt = str(info[i+4]) 
            mon = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", month_txt)[0]
            year_txt = str(info[i+5]) 
            yea = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", year_txt)[0]
            GB3 = ["GB3:GOV",cou, pri, yie, mon, yea]
            
        elif "GB6:GOV" in txt:
            coupon_txt = str(info[i+1])
            cou = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", coupon_txt)[0]
            price_txt = str(info[i+2]) 
            pri = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", price_txt)[-1]
            yield_txt = str(info[i+3]) 
            yie = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*",yield_txt)[0]
            month_txt = str(info[i+4]) 
            mon = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", month_txt)[0]
            year_txt = str(info[i+5]) 
            yea = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", year_txt)[0]   
            GB6=["GB6:GOV",cou, pri, yie, mon, yea]
    
        elif "GB12:GOV" in txt:
            coupon_txt = str(info[i+1])
            cou = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", coupon_txt)[0]
            price_txt = str(info[i+2]) 
            pri = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", price_txt)[-1]
            yield_txt = str(info[i+3]) 
            yie = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*",yield_txt)[0]
            month_txt = str(info[i+4]) 
            mon = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", month_txt)[0]
            year_txt = str(info[i+5]) 
            yea = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", year_txt)[0]
            GB12=["GB12:GOV",cou, pri, yie, mon, yea]
    
        elif "GT2:GOV" in txt:
            coupon_txt = str(info[i+1])
            cou = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", coupon_txt)[0]
            price_txt = str(info[i+2]) 
            pri = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", price_txt)[-1]
            yield_txt = str(info[i+3]) 
            yie = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*",yield_txt)[0]
            month_txt = str(info[i+4]) 
            mon = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", month_txt)[0]
            year_txt = str(info[i+5]) 
            yea = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", year_txt)[0]
            GT2=["GT2:GOV",cou, pri, yie, mon, yea]
    
        elif "GT5:GOV" in txt:
            coupon_txt = str(info[i+1])
            cou = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", coupon_txt)[0]
            price_txt = str(info[i+2]) 
            pri = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", price_txt)[-1]
            yield_txt = str(info[i+3]) 
            yie = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*",yield_txt)[0]
            month_txt = str(info[i+4]) 
            mon = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", month_txt)[0]
            year_txt = str(info[i+5]) 
            yea = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", year_txt)[0]
            GT5=["GT5:GOV",cou, pri, yie, mon, yea]
    
        elif "GT10:GOV" in txt:
            coupon_txt = str(info[i+1])
            cou = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", coupon_txt)[0]
            price_txt = str(info[i+2]) 
            pri = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", price_txt)[-1]
            yield_txt = str(info[i+3]) 
            yie = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*",yield_txt)[0]
            month_txt = str(info[i+4]) 
            mon = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", month_txt)[0]
            year_txt = str(info[i+5]) 
            yea = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", year_txt)[0]
            GT10=["GT10:GOV",cou, pri, yie, mon, yea]
    
        elif "GT30:GOV" in txt:
            coupon_txt = str(info[i+1])
            cou = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", coupon_txt)[0]
            price_txt = str(info[i+2]) 
            pri = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", price_txt)[-1]
            yield_txt = str(info[i+3]) 
            yie = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*",yield_txt)[0]
            month_txt = str(info[i+4]) 
            mon = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", month_txt)[0]
            year_txt = str(info[i+5]) 
            yea = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", year_txt)[0]
            GT30=["GT30:GOV",cou, pri, yie, mon, yea]
    
        elif "FDFD:IND" in txt:
            current_txt = str(info[i+1])
            cur = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", coupon_txt)[0]
            year_txt = str(info[i+2]) 
            yea = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", year_txt)[0]
            FDFD=["FDFD:IND",cur, yea]
    
        elif "FDTR:IND" in txt:
            current_txt = str(info[i+1])
            cur = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", coupon_txt)[0]
            year_txt = str(info[i+2]) 
            yea = re.findall("[-+]?\d+[\.]?\d*[eE]?[-+]?\d*", year_txt)[0]
            FDTR=["FDTR:IND",cur, yea]
    
    cur_time = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
    tre_lst = [GB3,GB6,GB12,GT2,GT5,GT10,GT30]
    fed_lst = [FDFD,FDTR]    
    label1= ["NAME", "COUPON","PRICE","YIELD","1MONTH","1YEAR"]
    BR_df = pd.DataFrame.from_records(tre_lst, columns = label1)
    BR_df['TIME'] = cur_time
    path1 = dy + "/bankrate.txt"
    path1_f = formulate_directory(path1)
    BR_df.to_csv(path1_f, index = None, sep = ',', mode = 'w')
    
    label2 = ["NAME","CURRENT","1YEAR"]
    FD_df = pd.DataFrame.from_records(fed_lst, columns = label2)
    FD_df['TIME'] = cur_time
    path2 = dy + "/fedrate.txt"
    path2_f = formulate_directory(path2)
    FD_df.to_csv(path2_f, index = None, sep = ',', mode = 'w')
    
    print("H3 bank-rate reading and fed-rate read finish.")
    return True    


# First thing everyday: update symbols and googlesheets accordingly.
    # one problem.,..when does the website update its sp500 info daily??!!
cur_date = datetime.date.today()
cur_date = cur_date.strftime("%Y%m%d") 

#if not os.path.exists(directory):
#    os.makedirs(directory)
#date = datetime.date.today()
#date = date.strftime("%Y%m%d") 

directory = r".../PycharmProjects/Option/"+str(cur_date)
if not os.path.exists(directory):
    os.makedirs(directory)

def daily_job():
    try:
        directory = r".../PycharmProjects/Option/"+str(cur_date)
        if not os.path.exists(directory):
            os.makedirs(directory)
        dy = directory
        if not os.path.exists(dy):
            os.makedirs(dy)
        symbols = D1_crawlsp(dy)
        #D2_update_quotes(symbols)
    except Exception:
        traceback.print_exc()
    return True

def hourly_job():
    try:
        dy = directory
        #[quote_df, index_info] = H1_read_quotes_wts(dy)
        H2_read_option_wts(dy)
        H3_read_bankrate(dy)
    except Exception:
        traceback.print_exc()        
    return True

def job():
    print(directory)
    print("I'm working..."+str(time.ctime()))
#
daily_job()
hourly_job()
###

schedule.clear()
schedule.every(5).minutes.do(job)
#schedule.every().day.at("23:00").do(daily_job)
#schedule.every().day.at("08:30").do(daily_job)
#schedule.every().day.at("09:30").do(hourly_job)
schedule.every().day.at("10:00").do(hourly_job)
#schedule.every().day.at("10:30").do(hourly_job)
schedule.every().day.at("11:00").do(hourly_job)
#schedule.every().day.at("11:30").do(hourly_job)
schedule.every().day.at("12:00").do(hourly_job)
#schedule.every().day.at("12:30").do(hourly_job)
schedule.every().day.at("13:00").do(hourly_job)
#schedule.every().day.at("13:30").do(hourly_job)
schedule.every().day.at("14:00").do(hourly_job)
#schedule.every().day.at("14:30").do(hourly_job)
schedule.every().day.at("15:00").do(hourly_job)
#schedule.every().day.at("15:30").do(hourly_job)
schedule.every().day.at("16:00").do(hourly_job)
#schedule.every().day.at("16:30").do(hourly_job)
##
print("job starting...")
while True:
    schedule.run_pending()
    time.sleep(1)

#[quote_df,index_info] = H1_read_quotes(symbols,test_dy)
#current = index_info[0]['time']

# Jul 25: test, whether the google sheet api crawl data will auto-update:
#     <---ans: Yes, it auto-updates, the gap is at lease 2 mins.
#test1 = H1_read_quotes(symbols,test_dy)    
#df1 = test1[0]
#time.sleep(121)
#test3 = H1_read_quotes(symbols,test_dy)    
#df3 = test3[0]
#print(df3 == df1)

# Jul28:
#quote_df.to_csv(path1,sep=',', header=None, index=None,mode ='w')
#np.savetxt(path2, quote_df.values, fmt='%s')
