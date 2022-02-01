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

print("json.load record_list: ", 
      type(record_list)) 
      #record_list)

# json.dumps produces a str(python) from the dict(python)
print("json.dumps record list: ", 
      type(json.dumps(record_list))) 
      #json.dumps(record_list,))

#works with <class 'list'>
#first_record = record_list[0]
#columns = list(first_record.keys())
#print("column names: ", list(first_record.keys()))
#print("column names: ", [list(x.keys()) for x in record_list][0])

#works with <class 'dict'>
print(json.dumps(record_list['members'][1]['secretIdentity']))

#df = pd.read_json('superheroes.json')

# access a one times nested json
normalized = pd.json_normalize(record_list, record_path = ['members'], meta = ['squadName', 'homeTown'])
print(normalized)

# access a two times nested json
normalized = pd.json_normalize(data=record_list['members'], record_path = ["powers"], meta = ['name'])
print(normalized)

connectionstring = """
	host=localhost 
	dbname=json 
	user=postgres 
	password=postgres
"""

# 'json_populate_recordset(psqlTable, jsonTable) is a postgres function. 
# It orders the objects in 'jsonTable' to match the columns
# in psqlTable

#sql1 = (
#  """
#  """)
#print("sql1: ", sql1)


# connect to the PostgreSQL server
conn = psycopg2.connect(connectionstring)
cur = conn.cursor()

# execute the statements
cur.execute(
  """
  CREATE TABLE if not exists superheroes(
    name varchar(255),
    age INTEGER
  ) 
  """)
  
#cur.execute(sql1)

#row =  cur.fetchone()

#while row is not None:
#    print(row)
#    row = cur.fetchone()

conn.commit()

# close communication with the PostgreSQL database server
cur.close()
