#!/usr/bin/python3

import psycopg2
import json
import pandas as pd
from sqlalchemy import create_engine

""" Conversation table:
    JSON      Python
    object    dict
    array     list
    string    str """

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

# subset of the normalized dataframe
dataFrame = normalized[["name","age","homeTown"]].head(3).sort_values(by=['name'])
print(dataFrame)

# access a two times nested json
#normalized = pd.json_normalize(data=record_list['members'], record_path = ["powers"], meta = ['name'])

# create_engine(dialect+driver://username:password@host:port/database)
alchemyEngine = create_engine('postgresql+psycopg2://postgres:postgres@127.0.0.1/json', pool_recycle=3600);

postgreSQLConnection = alchemyEngine.connect();

postgreSQLTable      = "superheroes1";

try:
    frame = dataFrame.to_sql(postgreSQLTable, postgreSQLConnection, if_exists='fail');

except ValueError as vx:
    print(vx)

except Exception as ex:  
    print(ex)

else:
    print("PostgreSQL Table %s has been created successfully."%postgreSQLTable);

finally:
    postgreSQLConnection.close();


