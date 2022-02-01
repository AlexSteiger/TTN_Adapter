#!/usr/bin/python3

import psycopg2
import json

""" Conversation table:
    JSON      Python
    object    dict
    array     list
    string    str """

# json.loads produces a JSON document to a python document (see conversation table)
with open("jsonexample2.json") as json_data:
  record_list = json.load(json_data)

print("json.load record_list: ", 
      type(record_list), 
      record_list)

# json.dumps produces a str(python) from the dict(python)
print("json.dumps record list: ", 
      type(json.dumps(record_list,)), 
      json.dumps(record_list, ))

connectionstring = """
	host=localhost 
	dbname=json 
	user=postgres 
	password=postgres
"""

# 'json_populate_recordset(psqlTable, jsonTable) is a postgres function. 
# It orders the objects in 'jsonTable' to match the columns
# in psqlTable

sql1 = (
  """
  select * from 
  json_populate_recordset(null::psql_table, '[{"color":"red","value":"#f00"}]');
  """)
print("sql1: ", sql1)

sql2 = (
  """
  select color, value from
  json_populate_recordset(NULL::psql_table, '%s');
  """) % json.dumps(record_list,)
print("sql2: ", sql2)

sql3 = (
  """
  insert into psql_table select * from
  json_populate_recordset(NULL::psql_table, '%s');
  """) % json.dumps(record_list,)
print(sql3)

# connect to the PostgreSQL server
conn = psycopg2.connect(connectionstring)
cur = conn.cursor()

# execute the statements
cur.execute(
  """
  CREATE TABLE if not exists psql_table(
    color varchar(255),
    value varchar(255)
  ) 
  """)
  
cur.execute(sql2)

row =  cur.fetchone()

while row is not None:
    print(row)
    row = cur.fetchone()

conn.commit()

# close communication with the PostgreSQL database server
cur.close()
