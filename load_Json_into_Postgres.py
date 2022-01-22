#!/usr/bin/python3

import json, sys

from psycopg2 import connect, Error

with open('response.json') as json_data:
    json_list = json.load(json_data)

# Check object type
print("json_list: ", type(json_list))

if type(json_list) == list:
    first_record = json_list[0]
    columns = list(first_record.keys())
    print ("\ncolumn names:", columns)
    



