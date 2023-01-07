from cmath import tan
from itertools import takewhile
from binance.client import Client
from bson import ObjectId
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


#API kEYS
API_KEY = os.environ.get("API_KEY")
API_SECRET = os.environ.get("API_SECRET")

#BINANCE CLIENT
BINANCE_CLIENT = Client(API_KEY, API_SECRET)


#SYMBOLS AND STREAMS FOR WEBSOCJKET
LIST_OF_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
LIST_OF_STREAMS = []
for stream in LIST_OF_SYMBOLS:
    LIST_OF_STREAMS.append("{}@kline_1m".format(stream.lower()))
    
    
#TIME FRAMES
TF_EQUIV = {"1m": 60, "5m": 300, "15m": 900,
            "30m": 900, "1h": 3600, "4h": 14400}
TF = "1m"
tf_equiv = TF_EQUIV[TF] * 1000
KLINE_INTERVAL_1MINUTE = '1m'
KLINE_INTERVAL_3MINUTE = '3m'
KLINE_INTERVAL_5MINUTE = '5m'
KLINE_INTERVAL_15MINUTE = '15m'
KLINE_INTERVAL_30MINUTE = '30m'
KLINE_INTERVAL_1HOUR = '1h'
KLINE_INTERVAL_2HOUR = '2h'
KLINE_INTERVAL_4HOUR = '4h'
KLINE_INTERVAL_6HOUR = '6h'
KLINE_INTERVAL_8HOUR = '8h'
KLINE_INTERVAL_12HOUR = '12h'
KLINE_INTERVAL_1DAY = '1d'
KLINE_INTERVAL_3DAY = '3d'
KLINE_INTERVAL_1WEEK = '1w'
KLINE_INTERVAL_1MONTH = '1M'


    
#CONNECTION TO MONGO DB
MONGODB_HOST = "mongodb://localhost:27017"
MONGODB_PORT = 27017
MONGO_CLIENT = MongoClient(MONGODB_HOST)
DB = MONGO_CLIENT["coins"]
DB_COINS = MONGO_CLIENT["t"]
TIME_FRAMES_CONVERSION = ['5T', '15T', '30T','60T', '120T', '240T', '480T', '1D']




