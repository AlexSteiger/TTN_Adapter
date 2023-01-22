#!/usr/bin/python3

#!/usr/bin/python3

#import psycopg2 #alternative to sqlalchemy to make a connection to psql
import time
import json
import requests
import pandas as pd
import geopandas
import datetime
from sqlalchemy import create_engine
import os.path

#run the first time only to create the table:

# Before running this script:
# --pandas version >1.4.0 needs to be installed
# --create postgresql database 'addferti_lorawan

# Postgres:
postgreSQLTable = ['ru_soil_moisture','bursa_soil_moisture','ugent_soil_moisture']
alchemyEngine   = create_engine('postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/addferti_lorawan', pool_recycle=3600);
                # create_engine(dialect+driver://username:password@host:port/database)

# TTN Application:
theApplication = ['addferti-rostock-soil-moisture','addferti-bursa-soil-moisture','addferti-ugent-soil-moisture']
theAPIKey = ['NNSXS.5IXRRQ74V3NDRIMSP4RQ6FZ5W5CEGL5P6QN457Q.JOIUJJ5TYRJDCMMHTZMH7HBGTPVLTHYQYZUYXFMHHOQ2WGW5DL4Q',
             'NNSXS.FC4XATDRAUL22VSYSZYB7XPJQXHLZI534GVKKAY.QM5FGNQX7B6DNWE4CVD5ZYUQ6HUFQP72KX5KWTOSNTIG4TTFJX6A',
             'NNSXS.R7NNSNQE24QDNLJ7XIQD6CRBVYHCWO72C7E2REY.JN2I3FWELVV2E6CZCIAEKXNJVLB6DTJZ34JOPXMOSFTYL4PWP4DA']

for i in range(0,3):
    
    # Note the path you have to specify. Double note that it has be prefixed with up.
    theFields = "up.uplink_message.decoded_payload,up.uplink_message.locations"

    theNumberOfRecords = 100

    theURL = "https://eu1.cloud.thethings.network/api/v3/as/applications/" + theApplication[i] + "/packages/storage/uplink_message?order=-received_at&limit=" + str(theNumberOfRecords) + "&field_mask=" + theFields

    # These are the headers required in the documentation.
    theHeaders = { 'Accept': 'text/event-stream', 'Authorization': 'Bearer ' + theAPIKey[i] }

    print("\nFetching data from ",theApplication[i])

    r = requests.get(theURL, headers=theHeaders)
    #print(r.text)

    print("URL: " + r.url)
    print("Status: " + str(r.status_code))
    
    theJSON = "{\"data\": [" + r.text.replace("\n\n", ",")[:-1] + "]}";

    df = pd.read_json(theJSON)
    try:
        normalized_df = pd.concat([pd.DataFrame(pd.json_normalize(x)) for x in df['data']],ignore_index=True)

            #print("column headers:")
            #for col in normalized_df.columns:
            #	  print(col)
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
            #result.uplink_message.locations.user.latitude       --> Lat
            #result.uplink_message.locations.user.longitude      --> Long
            #-------------------------------------------

        # subset of the normalized dataframe
        df = normalized_df[[
          "result.end_device_ids.device_id",
          "result.received_at",
          "result.uplink_message.decoded_payload.conduct_SOIL",
          "result.uplink_message.decoded_payload.temp_SOIL",
          "result.uplink_message.decoded_payload.water_SOIL",
          "result.uplink_message.locations.user.latitude",
          "result.uplink_message.locations.user.longitude"]]

        #df = df.reset_index()

        TTN_df = df.rename(columns={
          "result.end_device_ids.device_id":                    "device_id",
          "result.received_at":                                 "time",
          "result.uplink_message.decoded_payload.conduct_SOIL": "soil_ec",
          "result.uplink_message.decoded_payload.temp_SOIL":    "soil_temp",
          "result.uplink_message.decoded_payload.water_SOIL":   "soil_mc",
          "result.uplink_message.locations.user.latitude":      "lat",
          "result.uplink_message.locations.user.longitude":     "long"})

        TTN_df.time        = pd.to_datetime(TTN_df['time'])
        TTN_df.time        = TTN_df.time.round('S')
        TTN_df.soil_ec     = pd.to_numeric(TTN_df['soil_ec'])
        TTN_df.soil_temp   = pd.to_numeric(TTN_df['soil_temp'])
        TTN_df.soil_mc     = pd.to_numeric(TTN_df['soil_mc'])
        TTN_df             = TTN_df[TTN_df['soil_temp'] != 0]
        TTN_df.lat         = TTN_df.lat.round(8)
        TTN_df.long        = TTN_df.long.round(8)
        
        #print(TTN_df.dtypes)
        #print("Fetched data: ")
        #print(TTN_df)

        try:
            postgreSQLConnection = alchemyEngine.connect();
            TTN_df.to_sql(postgreSQLTable[i], postgreSQLConnection, index=False, if_exists='append');
            SQL = ("DELETE FROM %s t WHERE EXISTS (SELECT FROM %s WHERE device_id = t.device_id " 
                   "AND time = t.time AND ctid < t.ctid "
                   "order by time);")%(postgreSQLTable[i],postgreSQLTable[i])
            postgreSQLConnection.execute(SQL)
            
        except TypeError:
            print("create table", postgreSQLTable[i])
            TTN_df.to_sql(postgreSQLTable[i], postgreSQLConnection, index=False, if_exists='fail');

    except ValueError:
        print("Value Error. No Data from TTN to fetch for " + postgreSQLTable[i])
        pass
    
    finally:
        postgreSQLConnection.close();

## Export the current soil moisture data as a Shapefile
## Upload to Geonode works through copying the new shapefiles in the Geoserver Docker Container
## sudo docker cp /data/current_ru_soil_moisture geoserver4my_geonode:/geoserver_data/data/data/geonode
## Also the Layer and the Store need to be created once (execute )
for i in range(0,3):
    postgreSQLTable = ['ru_soil_moisture','bursa_soil_moisture','ugent_soil_moisture']
    alchemyEngine   = create_engine('postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/addferti_lorawan', pool_recycle=3600);

    SQL = ("SELECT DISTINCT ON (device_id) " 
           " device_id, time, soil_temp, soil_mc, soil_ec, lat, long "
           " FROM %s ORDER BY device_id, time desc;")%(postgreSQLTable[i])
    try:
        df_c_smc = pd.read_sql(SQL,con=alchemyEngine)
    except:
        print(postgreSQLTable[i] + ' table not available.')
    else:
        folder = 'current_'+ postgreSQLTable[i]
        isExist = os.path.exists(folder)
        if not isExist:
           # Create a new directory because it does not exist
           os.makedirs(folder)

        # pip install geopandas


        # Transform DataFrame into a GeoDataFrame
        gdf = geopandas.GeoDataFrame(
            df_c_smc, geometry=geopandas.points_from_xy(df_c_smc.long, df_c_smc.lat))

        # Add projection
        gdf.crs = 'epsg:4326'

        # Transform python datetime object to an string (shapefile cant read datetime format)
        gdf['time'] = gdf['time'].dt.strftime("%Y-%m-%dT%H:%M:%S")

        #print(gdf)
        #gdf.plot()

        # Export data to file
        print('exporting: current_' + postgreSQLTable[i] + '.shp')
        #df.to_csv  (folder + '/current_' + postgreSQLTable[i]+'.csv')
        gdf.to_file(folder + '/current_' + postgreSQLTable[i]+'.shp')
