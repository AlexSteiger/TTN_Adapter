#!/usr/bin/python3

#import psycopg2 #alternative to sqlalchemy to make a connection to psql
import json
import pandas as pd
from sqlalchemy import create_engine

df = pd.read_json('TTNresponse.json')
print("number of rows recieved: ",len(df))
print()

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
print()

# subset of the normalized dataframe
df = normalized_df[[
  "result.end_device_ids.device_id",
  "result.received_at",
  "result.uplink_message.decoded_payload.relative_humidity_2",
  "result.uplink_message.decoded_payload.temperature_1"]]

TTN_df = df.rename(columns={
  "result.end_device_ids.device_id":"device_id",
  "result.received_at":"recieved_at",
  "result.uplink_message.decoded_payload.relative_humidity_2":"relative_humidity",
  "result.uplink_message.decoded_payload.temperature_1":"temperature"})

print("Fetched data: ")
print(TTN_df)

# create_engine(dialect+driver://username:password@host:port/database)
alchemyEngine = create_engine('postgresql+psycopg2://postgres:postgres@127.0.0.1/json', pool_recycle=3600);

postgreSQLConnection = alchemyEngine.connect();

postgreSQLTable      = "ard_mrk_wan_1300";

#run the first time only to create the table:
#frame = TTN_df.to_sql(postgreSQLTable, postgreSQLConnection, if_exists='fail');

psql_df = pd.read_sql('select * from ard_mrk_wan_1300', con=postgreSQLConnection)

psql_df = psql_df.drop(columns=['index'])

df_unique = pd.concat([TTN_df,psql_df]).drop_duplicates(keep=False)

print(len(df_unique), "new rows added to the database")

frame = df_unique.to_sql(postgreSQLTable, postgreSQLConnection, if_exists='append');

postgreSQLConnection.close();


