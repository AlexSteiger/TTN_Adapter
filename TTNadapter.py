#!/usr/bin/python3

#import psycopg2 #alternative to sqlalchemy to make a connection to psql
import json
import requests
import pandas as pd
import datetime
from sqlalchemy import create_engine

#run the first time only to create the table:

# Before running this script:
# --pandas version >1.4.0 needs to be installed
# --create postgresql database 'addferti_lorawan
#Postgres:


postgreSQLTable = "ard_mrk_wan_1300";
alchemyEngine   = create_engine('postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/addferti_lorawan', pool_recycle=3600);
                # create_engine(dialect+driver://username:password@host:port/database)


theApplication = "arduino-mrk-wan-1300"
theAPIKey = "NNSXS.LFKZMYHWIZAXYJQL4TK6PER3CKXX4XNSZULY4EA.YJ7B4GEBZVEDLG6UPTQWVSHL6MMGUVBM4O25XTTBGWGKWO4BUPXA"

# Note the path you have to specify. Double note that it has be prefixed with up.
theFields = "up.uplink_message.decoded_payload,up.uplink_message.frm_payload"

theNumberOfRecords = 10

theURL = "https://eu1.cloud.thethings.network/api/v3/as/applications/" + theApplication + "/packages/storage/uplink_message?order=-received_at&limit=" + str(theNumberOfRecords) + "&field_mask=" + theFields

# These are the headers required in the documentation.
theHeaders = { 'Accept': 'text/event-stream', 'Authorization': 'Bearer ' + theAPIKey }

print("\n\nFetching from data storage  ...\n")

r = requests.get(theURL, headers=theHeaders)
#print(r.text)

print("URL: " + r.url)
print()
print("Status: " + str(r.status_code))
print()

#print("Response: ")
#print(r.text)
#print()

# Due to some design choices by TTI, the text returned is not proper JSON.
# The next line fixes that
theJSON = "{\"data\": [" + r.text.replace("\n\n", ",")[:-1] + "]}";

# Block to write and read the JSON to and from a file 
"""
parsedJSON = json.loads(theJSON)

with open('TTNresponse.json', 'w') as f:
	json.dump(parsedJSON, f)

print(theNumberOfRecords, "records fetched from TTN")

df = pd.read_json('TTNresponse.json')
print("number of rows recieved: ",len(df))
"""

df = pd.read_json(theJSON)

# solution from https://stackoverflow.com/questions/45325208/python-json-normalize-a-pandas-series-gives-typeerror
normalized_df = pd.concat([pd.DataFrame(pd.json_normalize(x)) for x in df['data']],ignore_index=True)

#print("column headers:")
	#for col in normalized_df.columns:
	#  print(col)
	#-------------------------------------------
	#result.end_device_ids.device_id
	#result.received_at
	#result.uplink_message.frm_payload
	#result.uplink_message.decoded_payload.relative_humidity_2
	#result.uplink_message.decoded_payload.temperature_1
	#result.uplink_message.received_at
	#-------------------------------------------

# subset of the normalized dataframe
df = normalized_df[[
  "result.end_device_ids.device_id",
  "result.received_at",
  "result.uplink_message.decoded_payload.relative_humidity_2",
  "result.uplink_message.decoded_payload.temperature_1"]]

#df = df.reset_index()

TTN_df = df.rename(columns={
  "result.end_device_ids.device_id":"device_id",
  "result.received_at":"recieved_at",
  "result.uplink_message.decoded_payload.relative_humidity_2":"relative_humidity",
  "result.uplink_message.decoded_payload.temperature_1":"temperature"})

TTN_df.recieved_at = pd.to_datetime(TTN_df['recieved_at'])
TTN_df.recieved_at = TTN_df.recieved_at.round('S')


print("Fetched data: ")
print(TTN_df)

postgreSQLConnection = alchemyEngine.connect();

try:
  print("try...")
  psql_df = pd.read_sql('select * from ard_mrk_wan_1300', con=postgreSQLConnection)
  #psql_df = psql_df.drop(columns=['index'])
  #print("psql_df: ")
  #print(psql_df)
  #print(TTN_df.compare(psql_df))
  df_unique = pd.concat([TTN_df,psql_df]).drop_duplicates(subset=['device_id','recieved_at'],keep=False)
  #print("df_unique: ")
  #print(df_unique)
  print(len(df_unique), "new rows added to the database")
  frame = df_unique.to_sql(postgreSQLTable, postgreSQLConnection, index=False, if_exists='append');
except:
  print("except...")
  print("create table", postgreSQLTable)
  frame = TTN_df.to_sql(postgreSQLTable, postgreSQLConnection, index=False, if_exists='fail');
finally:
  postgreSQLConnection.close();
