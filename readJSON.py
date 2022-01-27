#!/usr/bin/python3

# run in terminal with: $ python3 filename.py

import json, sys

from psycopg2 import connect, Error

# use Python's open() function to load the JSON data
with open('response.json') as json_data:
	# use json.load() for JSON files (and json.loads() for JSON strings)
	record_list = json.load(json_data)

print(json.dumps(record_list, indent=2))

if type(record_list) == list:
	first_record = record_list[0]
	# get the column names from the first record
	columns = list(first_record.keys())
	print ("\ncolumn names:", columns)						
