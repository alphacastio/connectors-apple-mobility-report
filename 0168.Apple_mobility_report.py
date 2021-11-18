#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

json_file = requests.get('https://covid19-static.cdn-apple.com/covid19-mobility-data/current/v3/index.json')
url_base = 'https://covid19-static.cdn-apple.com'
base_path = eval(json_file.content)['basePath']
csv_file = eval(json_file.content)['regions']['en-us']['csvPath']

csv_url = url_base + base_path + csv_file


df = pd.read_csv(csv_url)

df = df[df["geo_type"] == "country/region"]

del df["geo_type"]
del df["alternative_name"]
del df["sub-region"]
del df["country"]
df = df.rename(columns={"region": "country"})


df = df.melt(["country", "transportation_type"]).set_index(["country", "transportation_type", "variable"]).unstack("transportation_type").reset_index()
df.columns = ["country", "Date", "driving", "transit", "walking"]
df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
df = df.set_index("Date")

alphacast.datasets.dataset(168).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
