#!/usr/bin/python3

#import psycopg2 #alternative to sqlalchemy to make a connection to psql
import time
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

postgreSQLTable = ["ru_soil_moisture","bursa_soil_moisture","ugent_soil_moisture"];

theApplication = ["addferti-rostock-soil-moisture",
                  "addferti-bursa-soil-moisture",
                  "addferti-ugent-soil-moisture"]

theAPIKey = [
  "NNSXS.5IXRRQ74V3NDRIMSP4RQ6FZ5W5CEGL5P6QN457Q.JOIUJJ5TYRJDCMMHTZMH7HBGTPVLTHYQYZUYXFMHHOQ2WGW5DL4Q",
  "NNSXS.FC4XATDRAUL22VSYSZYB7XPJQXHLZI534GVKKAY.QM5FGNQX7B6DNWE4CVD5ZYUQ6HUFQP72KX5KWTOSNTIG4TTFJX6A",
  "NNSXS.R7NNSNQE24QDNLJ7XIQD6CRBVYHCWO72C7E2REY.JN2I3FWELVV2E6CZCIAEKXNJVLB6DTJZ34JOPXMOSFTYL4PWP4DA"]

for i in range(0, 3):
  print(postgreSQLTable[i])

  # create_engine(dialect+driver://username:password@host:port/database)
  alchemyEngine = create_engine('postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/addferti_lorawan');
                
  # Note the path you have to specify. Double note that it has be prefixed with up.
  theFields = "up.uplink_message.decoded_payload,up.uplink_message.frm_payload"

  theNumberOfRecords = 5000

  theURL = ("https://eu1.cloud.thethings.network/api/v3/as/applications/{}"
            "/packages/storage/uplink_message?order=-received_at&limit={}&field_mask={}"
            .format(theApplication[i],str(theNumberOfRecords),theFields))

  # These are the headers required in the documentation.
  theHeaders = { 'Accept': 'text/event-stream', 'Authorization': 'Bearer ' + theAPIKey[i] }

  print("\n\nFetching from data storage  ...\n")

  r = requests.get(theURL, headers=theHeaders)
  #print(r.text)

  print("URL: " + r.url)
  print()
  print("Status: " + str(r.status_code))
  print()

  theJSON = "{\"data\": [" + r.text.replace("\n\n", ",")[:-1] + "]}";

  df = pd.read_json(theJSON)
  
  normalized_df = pd.concat([pd.DataFrame(pd.json_normalize(x)) for x in df['data']],ignore_index=True)

  print("here")
  #print("column headers:")
	#for col in normalized_df.columns:
		#  print(col)
		#-------------------------------------------
		#result.end_device_ids.device_id                     --> Device ID
		#result.received_at                                  --> Timestamp
		#result.uplink_message.frm_payload
		#result.uplink_message.decoded_payload.Bat
		#result.uplink_message.decoded_payload.TempC_DS18B20
		#result.uplink_message.decoded_payload.conduct_SOIL  --> Soil Conductivity (uS/cm) (mikroSiemens/cm)
		      #result.uplink_message.decoded_payload.temp_SOIL     --> Soil Temperature (°C)
		      #result.uplink_message.decoded_payload.water_SOIL    --> Soil Moisture (0-100%)
		      #result.uplink_message.received_at
		#-------------------------------------------

  # subset of the normalized dataframe
  df = normalized_df[[
    "result.end_device_ids.device_id",
    "result.received_at",
    "result.uplink_message.decoded_payload.conduct_SOIL",
    "result.uplink_message.decoded_payload.temp_SOIL",
    "result.uplink_message.decoded_payload.water_SOIL"]]

  #df = df.reset_index()

  TTN_df = df.rename(columns={
    "result.end_device_ids.device_id":                    "device_id",
    "result.received_at":                                 "recieved_at",
    "result.uplink_message.decoded_payload.conduct_SOIL": "soil_ec",
    "result.uplink_message.decoded_payload.temp_SOIL":    "soil_temp",
    "result.uplink_message.decoded_payload.water_SOIL":   "soil_mc"})

  TTN_df.recieved_at = pd.to_datetime(TTN_df['recieved_at'])
  TTN_df.recieved_at = TTN_df.recieved_at.round('S')
  TTN_df.soil_ec     = pd.to_numeric(TTN_df['soil_ec'])
  TTN_df.soil_temp   = pd.to_numeric(TTN_df['soil_temp'])
  TTN_df.soil_mc     = pd.to_numeric(TTN_df['soil_mc'])
  TTN_df             = TTN_df[TTN_df['soil_temp'] != 0]

  #print(TTN_df.dtypes)

  print("Fetched data: ")
  print(TTN_df)

  postgreSQLConnection = alchemyEngine.connect();

  try:
    frame = TTN_df.to_sql(postgreSQLTable[i], postgreSQLConnection, index=False, if_exists='append');
    postgreSQLConnection.execute("DELETE FROM {} t WHERE EXISTS (SELECT FROM {} WHERE device_id = t.device_id AND recieved_at = t.recieved_at AND ctid < t.ctid order by recieved_at);".format(postgreSQLTable[i],postgreSQLTable[i]))
  except:
    print("except...")
    print("create table", postgreSQLTable[i])
    frame = TTN_df.to_sql(postgreSQLTable[i], postgreSQLConnection, index=False, if_exists='fail');
  finally:
    postgreSQLConnection.close();


