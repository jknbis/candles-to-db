
from helpers.constants import *
import time


#function that updates candles to database
def update_1m_candle_to_db(symbol, candle_obj):
    DB[symbol].update_one(
        {"timestamp": candle_obj["timestamp"]},
        {"$set": candle_obj},
        upsert=True
    )
    
#function that inserts candles to database
def insert_1m_candle_to_db(symbol, candle):
    DB[symbol].insert_one(candle)

#function that deletes every day 800 candles from database
async def get_history_candles(client, symbol, tf):
    # fetch history candlesticks
    candles = await client.get_klines(
        symbol=symbol, interval=tf)
    return candles

#function that deletes every day 800 candles from database
def delete_300_candles_from_db():
    # iterate through the symbols
    for symbol in LIST_OF_SYMBOLS:
         for tf in TIME_FRAMES_CONVERSION:
             if tf == '1D' or tf == '480T' or tf == '240T' or tf == '120T' or tf == '60T':
                 pass
             else:
                collection_name = symbol
                DB_COINS = MONGO_CLIENT[collection_name]
                #delete old candles from
                DB_COINS[symbol+"_"+tf].delete_many({'timestamp': {'$lt': int(time.time())-86400*800}})
                print("deleted 300 candles from ", symbol, tf)
             
             
        
#get the last candle from database
def get_last_candle_from_db(symbol):
    candle = None
    try:
        candle = DB[symbol].find_one(
            sort=[("timestamp", -1)])
        return candle

    except:
        print("Could not find the last candle for ", symbol)
        
def resample_stock_data(df_one,tf):
    #set column ["Date"] to index* => datetimeIndex
    df_one['timestamps'] = pd.to_datetime(df_one['timestamp'],unit='ms')
    df_one.set_index('timestamps', inplace=True)
    df_five = pd.DataFrame()
    #get the timestamp of the last 5 minute
    #convert to unix timestamp
    df_five["open"] = df_one["open"].resample(tf).first()
    df_five["close"] = df_one["close"].resample(tf).last()
    df_five["high"] = df_one["high"].resample(tf).max()
    df_five["low"] = df_one["low"].resample(tf).min()
    df_five["volume"] = df_one["volume"].resample(tf).sum()
    df_five["time"] = tf
    
    #assign each timestamp its timestamp
    #df_five["timestamp"] = df_five.index.astype(np.int64)
    df_five["timestamp"] = df_five.index.map(lambda x: x.timestamp())
    #deprecated
    #df_five["timestamp"]  = df_five.index.astype(int)
    df_five = df_five.to_dict(orient = 'records')
    
    return df_five[-1]

def get_history_candles_by_tf(tf, symbol):
    # get history candlesticks
    # convert time_frames to intervals 
    if tf == '5T':
        interval = KLINE_INTERVAL_5MINUTE
        return BINANCE_CLIENT.get_historical_klines(symbol, interval, '5 day ago')
    elif tf == '15T':
        interval = KLINE_INTERVAL_15MINUTE
        return BINANCE_CLIENT.get_historical_klines(symbol, interval, '15 day ago')
    elif tf == '30T':
        interval = KLINE_INTERVAL_30MINUTE
        return BINANCE_CLIENT.get_historical_klines(symbol, interval, '30 day ago')
    elif tf == '60T':
        interval = KLINE_INTERVAL_1HOUR
        return BINANCE_CLIENT.get_historical_klines(symbol, interval, '60 day ago')
    elif tf == '120T':
        interval = KLINE_INTERVAL_2HOUR
        return BINANCE_CLIENT.get_historical_klines(symbol, interval, '120 day ago')
    elif tf == '240T':
        interval = KLINE_INTERVAL_4HOUR
        return BINANCE_CLIENT.get_historical_klines(symbol, interval, '240 day ago')
    elif tf == '480T':
        interval = KLINE_INTERVAL_8HOUR
        return BINANCE_CLIENT.get_historical_klines(symbol, interval, '480 day ago')
    elif tf == '1D':
        interval = KLINE_INTERVAL_1DAY
        return BINANCE_CLIENT.get_historical_klines(symbol, interval, '1440 day ago')
    else:
        interval = KLINE_INTERVAL_1MINUTE


def insert_history_candles_to_db_2():
    print("Inserting history candles to db")
    #iterate through the symbols
    for symbol in LIST_OF_SYMBOLS:
        for tf in TIME_FRAMES_CONVERSION:
            klines = get_history_candles_by_tf(tf, symbol)
            # convert time stamp
            for k in klines:
                candle_obj = {
                    "timestamp": int(k[0]/1000),
                    'open': float(k[1]),
                    'high': float(k[2]),
                    'low': float(k[3]),
                    'close': float(k[4]),
                    'volume': float(k[5]),
                }
                # insert to mongo
                collection_name = symbol
                DB_COINS = MONGO_CLIENT[collection_name]
                DB_COINS[symbol+"_"+tf].insert_one(candle_obj)
            
            print("inserted ", symbol,"for timeframe -", tf , " total of -",len(klines), " candles to db")

