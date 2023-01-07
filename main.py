from ast import List
import asyncio
from ctypes.wintypes import tagPOINT
from datetime import datetime, timedelta
import time
from threading import Timer
from pymongo import MongoClient
from binance import AsyncClient, BinanceSocketManager, Client
import pandas as pd
import numpy as np
from helpers.constants import *
from helpers.candles_to_db_helper import *


# global constants
missed_candles = []
last_candle = None

#function that checks if the database is empty
def check_if_database_is_empty():
    if(len(DB.list_collection_names()) == 0):
        print("No collections found, creating collections...")
        print("Please be patient, and drink a cup of coffee, this will take a while...")
        for s in LIST_OF_SYMBOLS:
            DB.create_collection(s, capped=True, size=5242880, max=5000).create_index(
                [("timestamp", -1)], unique=True)
            # insert history data 1m 1440 candles
            klines = BINANCE_CLIENT.get_historical_klines(
                s, TF, "1 day ago")
            history_candles = []
            for k in klines:
                candle_obj = {'timestamp': int(k[0]),
                            'open': float(k[1]),
                            'high': float(k[2]),
                            'low': float(k[3]),
                            'close': float(k[4]),
                            'volume': float(k[5]),
                            }
                
                insert_1m_candle_to_db(s, candle_obj)
                
        insert_history_candles_to_db_2()
    else:
        print("Collections found, checking for missing candles...")
        # check if there are missing candles
        for s in LIST_OF_SYMBOLS:
            last_candle = get_last_candle_from_db(s)
            # get the last candle from the database
            # get the current time
            current_time = int(time.time() * 1000)
            # calculate the difference between the last candle and the current time
            time_diff = current_time - last_candle['timestamp']
            # if the difference is greater than 1m, then we have missing candles
            if time_diff > 60000:
                print("Missing candles for ", s)
                # get the number of missing candles
                num_of_candles = time_diff // 60000
                # get the missing candles
                klines = BINANCE_CLIENT.get_historical_klines(
                    s, TF, str(num_of_candles)+" minute ago")
                # add the missing candles to the database
                for k in klines:
                    candle_obj = {'timestamp': int(k[0]),
                                'open': float(k[1]),
                                'high': float(k[2]),
                                'low': float(k[3]),
                                'close': float(k[4]),
                                'volume': float(k[5]),
                                }
                    insert_1m_candle_to_db(s, candle_obj)
                print("Added missing candles for ", s, "total of candles: ", num_of_candles)
            else:
                print("No missing candles for ", s)

async def add_candles(close_price: float,
                      open_price: float,
                      high_price: float,
                      low_price: float,
                      volume: float,
                      timestamp: int,
                      symbol: str,
                      binance_client) -> str:
    # Calculate the difference between the current timestamp and the last session's timestamp.
    timestamp_diff = int(time.time() * 1000) - timestamp
    # Print the difference between the current time and the trade time.
    # if timestamp_diff >= 2000:
    # global variable for the last candle that was seen.
    global last_candle
    last_candle = get_last_candle_from_db(symbol)
    # Same Candle
    # If the timestamp is less than the last candle's timestamp plus the timeframe, then we have a new candle.
    # Update the last candle's values.
    if timestamp < last_candle['timestamp'] + tf_equiv:
        # Take the last candle from the data and add it to the dictionary.
        last_candle['close'] = close_price
        last_candle['volume'] = volume
        last_candle['open'] = open_price
        last_candle['high'] = high_price
        last_candle['low'] = low_price

        return "same_candle"

    # Missed Candles
    # If the timestamp is greater than the last candle's timestamp + 2 * tf_equiv, then we have
    # missed candles. We need to grab the last candle from the missed candles list and use that
    # as our new last candle. We also need to update the timestamp to be the timestamp of the
    # candle we grabbed.
    elif timestamp >= last_candle['timestamp'] + 2 * tf_equiv:

        # Given a symbol, a time frame, and a timestamp, determine how many candles are missing.
        missing_candles = int(
            (timestamp - last_candle['timestamp']) / tf_equiv) - 1
        print("'{0}', missing candles for '{1}',('{3}','{4}','{5}')".format(missing_candles, symbol,
                                                                            TF, timestamp, last_candle['timestamp']))
        # Get the candles from the Binance API.
        temp_missed_candles = await get_history_candles(binance_client, symbol, TF)
        # Add the candles to the missed candles list.
        temp_missed_candles.reverse()

        global missed_candles
        #taking the last missing_candles elements of the list.
        #Finally, the missed_candles variable is set to the sliced list.
        missed_candles = temp_missed_candles[-missing_candles]
        # Update the Last Candles
        print(missed_candles)
        # Create -New- Candle Object
        candle_info = {'timestamp': temp_missed_candles[-1][0],
                       'open': float(temp_missed_candles[-1][1]),
                       'high': float(temp_missed_candles[-1][2]),
                       'low': float(temp_missed_candles[-1][3]),
                       'close': float(temp_missed_candles[-1][4]),
                       'volume': float(temp_missed_candles[-1][5]),
                       }

        last_candle = candle_info

        return "missed_candles"

    # New Candle
    # If the timestamp is greater than the last candle's timestamp plus the timeframe,
    # then we have a new candle. Return the new candle. Otherwise, return the last candle.
    elif timestamp >= last_candle['timestamp'] + tf_equiv:
        # Create New TimeStamp
        new_ts = last_candle['timestamp'] + tf_equiv
        # Create -New- Candle Object
        candle_info = {'timestamp': new_ts,
                       'open': open_price,
                       'high': high_price,
                       'low': low_price,
                       'close': close_price,
                       'volume': volume}

        last_candle = candle_info
        return "new_candle"
    
    
async def getAllData(client, bsm):
    # For each symbol in the list of symbols, grab the candles from the API and insert them into the database.
    # for symbol in LIST_OF_SYMBOLS:
    #     symbol_candles = await get_history_candles(client, symbol, TF)
    #     #  Create a dictionary of candle objects for the last candle in the symbol_candles list.
    #     candle_obj = {'timestamp': symbol_candles[-1][0],
    #                   'open': float(symbol_candles[-1][1]),
    #                   'high': float(symbol_candles[-1][2]),
    #                   'low': float(symbol_candles[-1][3]),
    #                   'close': float(symbol_candles[-1][4]),
    #                   'volume': float(symbol_candles[-1][5]),
    #                   }
    #     #
    #      #check  if you have a collection called DB[symbol] else update
    #     if not DB[symbol].find_one():
    #         # Insert a candle object into the database.
    #         insert_1m_candle_to_db(symbol, candle_obj)
    #     else:
    #         # Update the candle object in the database.
    #         update_1m_candle_to_db(symbol, candle_obj)
    # print("Inserted {0} candles for {1}".format(len(symbol_candles), symbol))
    #start the websocket connection to binance and start listening for new candles    
    async with bsm.multiplex_socket(LIST_OF_STREAMS) as stream:
        print("Listening for new candles...")
        while True:
            # Receive the data from the stream.
            res = await stream.recv()
            # Take the stream name and return the symbol for the company that owns the stream.
            symbol = res['stream'].split("@")[0].upper()#BTCUSDT@kline_1m
            for s in LIST_OF_SYMBOLS:
                if symbol == s:
                    res = await add_candles(
                        float(res['data']['k']['c']),#close
                        float(res['data']['k']['o']),#open
                        float(res['data']['k']['h']),#high
                        float(res['data']['k']['l']),#low
                        float(res['data']['k']['v']),#volume
                        res['data']['k']['T'], symbol, client)
                    
            if res == "missed_candles":
                print("%s missed candles" % symbol)
                for candle in missed_candles:
                    insert_1m_candle_to_db(symbol, candle)
            #  If the candle is new, insert it into the database. If it is not new, do nothing.
            
            elif res == "new_candle":
                  for s in LIST_OF_SYMBOLS:
                        if s == symbol:
                            insert_1m_candle_to_db(symbol, last_candle)
                            print("Inserted new candle for {0}".format(symbol))
                            for t in TIME_FRAMES_CONVERSION:
                                # get the last two candles for DB_COINS
                                DB_COINS = MONGO_CLIENT[symbol]
                                two_candles = DB_COINS[symbol+'_'+t].find().sort(
                                        "timestamp", -1).limit(2)
                                first_candle = two_candles[0]['timestamp']
                                second_candle = two_candles[1]['timestamp']
                          
                                # convert timestamp to datetime
                                time_1 = datetime.fromtimestamp(
                                        first_candle)
                                time_2 = datetime.fromtimestamp(
                                        second_candle)
                                time_3 = datetime.fromtimestamp(
                                        last_candle['timestamp']/1000)
                                # calculate the time_interval between the two candles  
                                time_interval = time_2 - time_1 if time_2 > time_1 else time_1 - time_2
                                time_interval2 = time_3 - time_1 if time_3 > time_1 else time_1 - time_3
                                # convert time_interval to minutes
                                td_mins = int(
                                        round(time_interval.total_seconds() / 60))
                                td_mins2 = int(
                                        round(time_interval2.total_seconds() / 60))
                                # if the time_interval is greater than the timeframe, then we need to resample the data    
                                if(int(td_mins)==int(td_mins2)):
                                    # get all the candles for the symbol
                                    all_candles = DB[symbol].find().sort(
                                        "timestamp", -1)
                                    # convert the cursor to a dataframe
                                    c = pd.DataFrame(all_candles)
                                    c = resample_stock_data(c,t)
                                    candle_obj = {
                                                "timestamp": int(c['timestamp'])/1000000000,
                                                'open': float(c['open']),
                                                'high': float(c['high']),
                                                'low': float(c['low']),
                                                'close': float(c['close']),
                                                'volume': float(c['volume']),
                                        }
                                    res=DB_COINS[symbol+"_"+t].insert_one(candle_obj)
                                    
                            
                            
            elif res == "same_candle":
                for s in LIST_OF_SYMBOLS:
                     if s == symbol:
                         print("same candle for %s" % symbol, 
                               # convert timestamp to datetime
                               "timestamp",  datetime.fromtimestamp(
                                 last_candle['timestamp']/1000),
                                "open", last_candle['open'],
                               )
                         for t in TIME_FRAMES_CONVERSION:
                                # get the last candle for DB_COINS
                                DB_COINS = MONGO_CLIENT[symbol]
                                # get the last candle for DB_COINS
                                found_candle = DB_COINS[symbol+'_'+t].find().sort(
                                    "timestamp", -1).limit(1)
                              
                                # check if data is empty before
                                # update that candle to last_candle
                                high = found_candle[0]['high'] if found_candle[0][
                                    'high'] > last_candle['high'] else last_candle['high']
                                low = found_candle[0]['low'] if found_candle[0]['low'] < last_candle['low'] else last_candle['low']
                                last = {
                                    # convert timestamp to datetime
                                    "timestamp": (found_candle[0]['timestamp']),
                                    "open": found_candle[0]['open'],
                                    "high": high,
                                    "low": low,
                                    "close": last_candle['close'],
                                    "volume": found_candle[0]['volume']+last_candle['volume']
                                }
                                # update that candle in database
                                DB_COINS[symbol+"_" + t].find_one_and_update({"_id": found_candle[0]['_id']},
                                                                             {"$set": last})
                                

async def main():
    # Connect to the Binance API and get all the data.
    binance_client = await AsyncClient.create(api_key=API_KEY, api_secret=API_SECRET)
    print("Successfully connected to Binance API.")
    # Create a BinanceSocketManager to handle the sockets.
    print("Service started")
    check_if_database_is_empty()
    bm = BinanceSocketManager(binance_client)
    # call delete_old_candles_from_db
    # start any sockets here, i.e a trade socket
    await asyncio.gather(getAllData(binance_client, bm))

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
    print("End of loop!")
