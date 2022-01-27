#!/usr/bin/python3

import psycopg2
import json

with open("jsonexample1.json") as json_data:
  record_list = json.load(json_data)

print(type(record_list))

connectionstring = """
	host=localhost 
	dbname=json 
	user=postgres 
	password=postgres
"""

sql = (
  """
  insert into psql_table select * from
  json_populate_recordset(NULL::psql_table, %s);
  """
)

# 'json_populate_recordset' is a postgres function. 
# Expands the object in 'record_list' to a row 
# whose columns match the record type defined by 'psql_table'

sql1 = (
  """
  select * from
  json_populate_recordset(NULL::psql_table, %s);
  """
)

# connect to the PostgreSQL server
conn = psycopg2.connect(connectionstring)
cur = conn.cursor()

# execute the statements
cur.execute(
  """
  CREATE TABLE if not exists psql_table(
    color varchar(255), 
    value varchar(255)) 
  """)

#cur.execute(sql1, (json.dumps(record_list,)))
cur.execute("""
  select * from json_populate_recordset(
  null::psql_table, '[{"color":"red","value":"#f00"}]');
  """)

row =  cur.fetchone()

while row is not None:
    print(row)
    row = cur.fetchone()

conn.commit()

# close communication with the PostgreSQL database server
cur.close()
