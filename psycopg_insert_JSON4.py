#!/usr/bin/python3

#import psycopg2 #alternative to sqlalchemy to make a connection to psql
import json
import pandas as pd
from sqlalchemy import create_engine

# json.loads produces a JSON document to a python document (see conversation table)
with open("superheroes.json") as json_data:
  record_list = json.load(json_data)

print("json.load record_list: ", type(record_list)) 

# json.dumps produces a str(python) from the dict(python)
print("json.dumps record list: ",type(json.dumps(record_list))) 

# for completely flat jsons: df = pd.read_json('superheroes.json')

# access a one times nested json
normalized = pd.json_normalize(record_list, record_path = ['members'], meta = ['squadName', 'homeTown'])
print(normalized)

# access a two times nested json
#normalized = pd.json_normalize(data=record_list['members'], record_path = ["powers"], meta = ['name'])

# subset of the normalized dataframe
df = normalized[["name","age","homeTown"]].head(4).sort_values(by=['name'])
print("incoming_df:\n", df)

# create_engine(dialect+driver://username:password@host:port/database)
alchemyEngine = create_engine('postgresql+psycopg2://postgres:postgres@127.0.0.1/json', pool_recycle=3600);

postgreSQLConnection = alchemyEngine.connect();

postgreSQLTable      = "superheroes1";

psql_df = pd.read_sql('select * from superheroes1', con=postgreSQLConnection)

psql_df = psql_df[["name", "age", "homeTown"]]
print("psql_df:\n",psql_df)

df_unique = pd.concat([df,psql_df]).drop_duplicates(keep=False)

print("df_new:\n", df_unique)

frame = df_unique.to_sql(postgreSQLTable, postgreSQLConnection, if_exists='append');


postgreSQLConnection.close();


