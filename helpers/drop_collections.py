from constants import *
from pymongo import MongoClient

#
client = MongoClient('mongodb://localhost:27017/')

#
def drop_collections():
    client.drop_database('coins')
    client.drop_database('BNBUSDT')
    client.drop_database('BTCUSDT')
    client.drop_database('ETHUSDT')
    print("Collections deleted")
    
    
drop_collections()