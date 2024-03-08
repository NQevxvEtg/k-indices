import os
from ftplib import FTP
import socket
from io import StringIO, BytesIO
import pandas as pd
import io
from functools import reduce
from collections import deque
from collections import OrderedDict
import numpy as np
import matplotlib.pyplot as plt

import datetime

today = datetime.date.today()
quarter = (today.month - 1) // 3 + 1
year = today.year
print("Current year is", year)
print("Today is in quarter", quarter)



path = "./k-indices/"
pathOut = "./export/"

if not os.path.exists(path):
    os.makedirs(path)
if not os.path.exists(pathOut):
    os.makedirs(pathOut)


#######################################################################################
# update here
files =(
str(year) + 'Q' + str(quarter) + '_DGD.txt',
str(year) + 'Q' + str(quarter) + '_DSD.txt',        
)
#######################################################################################

def getkindices(file):
    r = BytesIO()
# ftp://ftp.swpc.noaa.gov/pub/indices/old_indices/
    ftp = FTP('ftp.swpc.noaa.gov')
    ftp.login()
    ftp.af = socket.AF_INET6
    ftp.cwd('pub/indices/old_indices/')
    ftp.retrbinary('RETR ' +  file, r.write)
    ftp.quit()

    r.seek(0)
    byte_str =  r.read()
    df = byte_str.decode('UTF-8')
    save = ( path + file)
    with open(save, 'w') as f:
        f.write(df)
        
# disable download
for file in files:
    getkindices(file)



pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

years = (range(2008,year+1)) # change this after new year
ki = ("DGD",
#       "DPD",
      "DSD",
     )


def kindex(year,q,i):

    print(year,q)
    print(i)

    if year < 2019:
        df = pd.read_csv(path + str(year) + '_' + i + '.txt',
                         delimiter= '\t', index_col=False, header=None, engine='python')
    if year >= 2019:
        df = pd.read_csv(path + str(year) + 'Q' + str(q) + '_' + i + '.txt',
                         delimiter= '\t', index_col=False, header=None, engine='python')


    df = df[df[0].str.startswith(('2'))]


    df = df.replace('-', ' -', regex=True)


    s = io.StringIO()
    df.to_string(s,index=False,header=False)
    s.seek(0)

    df =  s.read()

    df = io.StringIO(df)

    df = pd.read_csv(df, sep="\s+", header=None)

    df.columns=[i + str(x) for x in range(len(df.columns))]
    df = df.rename(columns={i + "0": "year",
                            i + "1": "month",
                            i + "2": "day", })

    df["timestamp"] = pd.to_datetime(df[["year","month","day"]])
    # print(df["timestamp"])

    df = df.drop(["year","month","day"] , axis='columns')

#     if i == "DSD":
#         df = df.drop(["DSD8"] , axis='columns')
    if i == "DSD":
#         print(df["DSD8"])
        if df["DSD8"].str.contains('A').any():
            df["DSD8"] = df["DSD8"].replace('A', '0', regex=True)
        if df["DSD8"].str.contains('B').any():
            df["DSD8"] = df["DSD8"].replace('B', '1', regex=True)
        if df["DSD8"].str.contains('C').any():
            df["DSD8"] = df["DSD8"].replace('C', '2', regex=True)
        if df["DSD8"].str.contains('M').any():
            df["DSD8"] = df["DSD8"].replace('M', '3', regex=True)
        if df["DSD8"].str.contains('X').any():
            df["DSD8"] = df["DSD8"].replace('X', '4', regex=True)
        if df["DSD8"].str.contains('\*').any():
            df["DSD8"] = df["DSD8"].replace('\*', '0', regex=True)
        if df["DSD8"].str.contains('Unk').any():
            df["DSD8"] = df["DSD8"].replace('Unk', '0', regex=True)

    df = df.set_index('timestamp')

    df = df.fillna(0)
    print(len(df),len(df.columns))
    kix.append(df)



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
kyears = []
for year in years:
    print(year)
    if year < 2019:
        q = None
        kix = []
        deque(map(lambda i: kindex(year,q,i),ki))
        df = reduce(lambda  left,right: pd.merge(left,right,on=['timestamp'],
                                                how='outer'), kix)
        kyears.append(df)

    if year >= 2019:
        try:
            for q in range(1,5):
                kix = []
                deque(map(lambda i: kindex(year,q,i),ki))
                df = reduce(lambda  left,right: pd.merge(left,right,on=['timestamp'],
                                                        how='outer'), kix)
                kyears.append(df)
        except:
            continue

df = pd.concat(kyears)
df1 = df.astype(float)

df1.columns = [
"FA","F1","F2","F3","F4","F5","F6","F7","F8",
"CA","C1","C2","C3","C4","C5","C6","C7","C8",
"PA","P1","P2","P3","P4","P5","P6","P7","P8",

"Radio-Flux-10.7cm","SESC-Sunspot-Number","Sunspot-Area-10E-6-Hemis.",
"New-Regions","Stanfor-Solar-Mean-Field","GOES15-X-Ray-Bkgd-Flux",
"X-Ray-C","X-Ray-M","X-Ray-X","X-Ray-S",
"Optical-1","Optical-2","Optical-3"
             ]


out = (pathOut + "k-indices.csv")
df1.to_csv(out,index=True)


print("\nk-indices")
