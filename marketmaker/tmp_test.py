from marketmaker.BitAssetAPI import  BitAssetDealsAPI
from concurrent.futures import ThreadPoolExecutor
from marketmaker.dbOperation.Sqlite3 import Sqlite3
import json
if __name__ == "__main__":
    listen_host = "wss://api.huobi.pro/ws"  # if okcoin.cn  change url wss://real.okcoin.cn:10440/websocket/okcoinapi
    listenPair_ContractId = '10' #CONTRACT_ID_dict = {'ETH/BTC':'10','BCH/BTC':'11','LTC/BTC':'12'}
    QUANTITY = 0.001 ## ETH/BTC:0.001 BCH/BTC:0.001  LTC/BTC:0.01
    sql3 = Sqlite3(dataFile="/mnt/data/bitasset/bitasset_test.sqlite")

    # test004 @ bitasset.com
    APIKEY = 'akfb93b97cffef4c0b'
    SECRETKEY = 'affc4258411e4439bdb6142e0e27fbe1'
    RESTURL = 'http://tapi.bitasset.cc:7005/'
    dealApi = BitAssetDealsAPI(RESTURL, APIKEY, SECRETKEY)
    executor_cancel = ThreadPoolExecutor(100)
    DEPTH = 15
    SPREAD = 0.1
    side = {'buy': '1', 'sell': '-1'}
    result2 = dealApi.trade(listenPair_ContractId, side['buy'], 0.040000, 0.122, '1')
    data2_dict = json.loads(result2)
    print(result2)


