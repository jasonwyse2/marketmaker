# from bitasset_util import BitAsset
from apscheduler.schedulers.blocking import BlockingScheduler
import time
from MarketMakerBasic import WebSocketBasic
# symbol1 = 'ETH-BTC'
# symbol2 = 'BCH-BTC'
# symbol3 = 'LTC-BTC'
symbolPair_list=['ETH/BTC','BCH/BTC','LTC/BTC']
bitasset = BitAsset()
def collect_order():
    bitasset_obj = bitasset
    time1 = time.time()
    num_to_delete_sql3 = 100
    bitasset_obj.collect_data_from_sql3_into_mongodb(symbolPair_list[0], num_to_delete_sql3)
    bitasset_obj.collect_data_from_sql3_into_mongodb(symbolPair_list[1], num_to_delete_sql3)
    bitasset_obj.collect_data_from_sql3_into_mongodb(symbolPair_list[2], num_to_delete_sql3)
    time2 = time.time()
    print('insert %d orders costs:(seconds):' % num_to_delete_sql3, time2 - time1)
def record_balance():
    bitasset_obj = bitasset
    localtime = time.asctime(time.localtime(time.time()))
    print('time:', localtime)
    bitasset_obj.record_balance(symbolPair_list[0])
    bitasset_obj.record_balance(symbolPair_list[1])
    bitasset_obj.record_balance(symbolPair_list[2])

if __name__ == "__main__":

    # collect_order()
    while True:
        try:
            # collect_order()
            record_balance()
            time.sleep(5)
        except:
            pass