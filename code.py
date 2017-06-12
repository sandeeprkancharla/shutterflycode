# -*- coding: utf-8 -*-

import pandas as pd
from datetime import datetime, date, time
import numpy as np

from catalyst.pandas.convert import to_datetime, to_float, to_unicode
from catalyst.pandas.headers import get_clean_names

# Read data from events.txt

filename = u'C:/Users/Deepu/Documents/events.txt'
data_frame = pd.read_table(
    filename,
    delimiter=',', encoding='ascii', skiprows=0,
    na_values=None, comment=None, header=0,
    thousands=None, skipinitialspace=True, mangle_dupe_cols=False
)

# Ensure stripping and uniqueness of column names
data_frame.columns = get_clean_names(data_frame.columns)

# Type conversion for the following columns: last_name, camera_make, ...
for column in [u'last_name', u'camera_make', u'tags', u'camera_model', u'verb', u'adr_state', u'adr_city', u'type']:
    data_frame[column] = to_unicode(data_frame[column], encoding='utf-8')

# Convert total_amount to float
data_frame[u'total_amount'] = to_float(data_frame[u'total_amount'])

# Convert event_time to datetime
data_frame[u'event_time'] = to_datetime(data_frame[u'event_time'])

# using lists to add the data

customers = []
custKeyList = []
evtTmeList = []
lnameList = []
adrCityList = []
adrStateList = []
vstKeyList = []
vstTagList = []
ordKeyList = []
ttlAmtList = []
imgKeyList = []
cmrMkeList = []
cmrMdlList = []
D = data_frame

def Ingest(e, D):
    for x in D:
        if x[0] == "CUSTOMER" :
            custKeyList.append(x[1])
            evtTmeList.append(x[2])
            lnameList.append(x[3])
            adrCityList.append(x[4])
            adrStateList.append(x[5])
            return custKeyList, evtTmeList, lnameList, adrCityList, adrStateList
            break
        elif x[0] == "SITE_VISIT":
            vstKeyList.append(x[1])
            evtTmeList.append(x[2])
            custKeyList.append(x[3])
            vstTagList.append(x[4])
            return vstKeyList, evtTmeList, custKeyList, vstTagList
            break
        elif x[0] == "ORDER":
            ordKeyList.append(x[1])
            evtTmeList.append(x[2])
            custKeyList.append(x[3])
            ttlAmtList.append(x[4])
            return ordKeyList, evtTmeList, custKeyList, ttlAmtList
            break
        elif x[0] == "IMAGE":
            imgKeyList.append(x[1])
            evtTmeList.append(x[2])
            custKeyList.append(x[3])
            cmrMkeList.append(x[4])
            cmrMdlList.append(x[5])
            return imgKeyList, evtTmeList, custKeyList, cmrMkeList, cmrMdlList
            break
        
def TopXSimpleLTVCustomers(x, D):
    custKeyList = []
    expenList = []
  # we can filter the Data for specific week if needed by datetime.date(datetime.strptime(x["event_time"].split(":")[0], "%Y-%m-%d")).isocalendar()[1] 
  #which returns week number in teh calander year
    for c in custKeyList:
        custKeyList.append(c["key"])
        expenList.append(
        x= sum([float(cs["total_amount"].split(" ")[0]) for cs in ordKeyList if cs["customer_id"] == c["key"]])*52 +10 )
    
    expenList = np.array(expenList)
    
    TopXLTVIndex = expenList.argsort()[-1*x:][::-1]
    
    TopXLTV = [custKeyList[i] for i in TopXLTVIndex]
    
    return TopXLTV

uptDta = Ingest(1, D)
print(TopXSimpleLTVCustomers(2, uptDta))