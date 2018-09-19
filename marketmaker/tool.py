from datetime import datetime,timedelta
import time
import configparser
from marketmaker.MongoOps import Mongo
from marketmaker.UserInfo_Conf import UserName_UserId_dict
def from_timestamp13_to_localtime(timestamp):
    local_str_time = datetime.fromtimestamp(timestamp / 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
    return str(local_str_time)[:-3]
def from_timestamp10_to_localtime(timestamp,format_str = '%Y-%m-%d %H:%M:%S'):
    local_str_time = datetime.fromtimestamp(timestamp).strftime(format_str)
    return str(local_str_time)
def get_timestamp10_minutes_ago(num):
    days_ago = (datetime.now() - timedelta(minutes=num))
    timeStamp = int(time.mktime(days_ago.timetuple()))
    return timeStamp
def get_timestamp_from_time_str(time_str):
    st = time.strptime(time_str, '%Y-%m-%d %H:%M:%S')

    timestamp = int(time.mktime(st))*1000
    return timestamp
def get_local_datetime(format_str='%Y-%m-%d %H:%M:%S'):
    timestamp10 = time.time()
    tl = time.localtime(timestamp10)
    format_time = time.strftime(format_str, tl)
    return format_time

def get_config_parameter_dict(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    config_parameter_dict = {}
    secs = config.sections()
    for sec in secs:
        items = config.items(sec)
        for (k, v) in items:
            config_parameter_dict[k] = v
    return config_parameter_dict
def get_dealApi(userName):
    mongo_obj = Mongo()
    Mongodb_userTable = mongo_obj.get_mongodb_table(mongodb_name='bitasset', mongoTable_name='user')
    user_obj = Mongo.User(Mongodb_userTable)
    userId = UserName_UserId_dict[userName]
    dealApi = user_obj.get_dealApi(userId)
    return dealApi
if __name__ == "__main__":
    time_str = '2018-09-01 23:11:01'
    ts = 1537161276200
    print(from_timestamp13_to_localtime(ts))
    # print(get_timestamp_from_time_str(time_str))