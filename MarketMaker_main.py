if __name__ == "__main__":
    listen_host = "wss://api.huobi.pro/ws"  # if okcoin.cn  change url wss://real.okcoin.cn:10440/websocket/okcoinapi
    listenPair_ContractId = '10' #CONTRACT_ID_dict = {'ETH/BTC':'10','BCH/BTC':'11','LTC/BTC':'12'}
    QUANTITY = 0.001 ## ETH/BTC:0.001 BCH/BTC:0.001  LTC/BTC:0.01
    sql3_dataFile = "/mnt/data/bitasset/bitasset0906.sqlite"

    # test004 @ bitasset.com
    userName = 'test004'
    mongo_obj = Mongo()
    Mongodb_userTable = mongo_obj.get_mongodb_table(mongodb_name='bitasset',mongoTable_name='user')
    user_obj = Mongo.User(Mongodb_userTable)
    dealApi = user_obj.get_dealApi(UserName_UserId_dict[userName])

    DEPTH = 15
    SPREAD = 0.1

    mkt_mkr = MarketMaker(listen_host, listenPair_ContractId, sql3_dataFile, dealApi, DEPTH,QUANTITY,SPREAD)
    mkt_mkr.run()