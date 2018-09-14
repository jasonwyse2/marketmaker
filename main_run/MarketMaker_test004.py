from marketmaker.MongoOps import Mongo
from marketmaker.MarketMaker_0913 import MarketMaker
from marketmaker.dbOperation.UserInfo_Conf import UserName_UserId_dict
if __name__ == "__main__":
    listen_host = "wss://api.huobi.pro/ws"  # if okcoin.cn  change url wss://real.okcoin.cn:10440/websocket/okcoinapi
    listenPair_ContractId = '10' #CONTRACT_ID_dict = {'ETH/BTC':'10','BCH/BTC':'11','LTC/BTC':'12'}
    QUANTITY = 0.001 ## ETH/BTC:0.001 BCH/BTC:0.001  LTC/BTC:0.01

    # ------------- set 'userName' and 'sql3_dataFile' together -----------------
    userName = 'test004'
    sql3_dataFile = "/mnt/data/bitasset/bitasset0906.sqlite"
    # ------------- set 'userName' and 'sql3_dataFile' together -----------------

    mongo_obj = Mongo()
    Mongodb_userTable = mongo_obj.get_mongodb_table(mongodb_name='bitasset',mongoTable_name='user')
    user_obj = Mongo.User(Mongodb_userTable)
    userId = UserName_UserId_dict[userName]
    dealApi = user_obj.get_dealApi(userId)
    DEPTH = 15
    SPREAD = 0.1
    THICK_DEPTH = 15
    mkt_mkr = MarketMaker(listen_host, listenPair_ContractId, userId,sql3_dataFile,
                          dealApi, DEPTH,QUANTITY,SPREAD,THICK_DEPTH)
    mkt_mkr.run()