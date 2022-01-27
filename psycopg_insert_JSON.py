#!/usr/bin/python3

import psycopg2
import json

with open("jsonexample1.json") as json_data:
  record_list = json.load(json_data)

print(type(record_list))

connectionstring = """
	host=localhost 
	dbname=suppliers 
	user=postgres 
	password=postgres
"""

sql = (
  """
  insert into json_table select * from
  json_populate_recordset(NULL::json_table, %s);
  """
)

# connect to the PostgreSQL server
conn = psycopg2.connect(connectionstring)
cur = conn.cursor()

# execute the statements
cur.execute(
  """
  CREATE TABLE if not exists json_table(
    color varchar(255), 
    value varchar(255)) 
  """)

cur.execute(sql, (json.dumps(record_list,)))

conn.commit()

# close communication with the PostgreSQL database server
cur.close()
