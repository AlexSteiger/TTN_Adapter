#!/usr/bin/python3

# run in terminal with: $ python3 Descartes_Data_Storage_download_template.py

import requests
import json

theApplication = "arduino-mrk-wan-1300"
theAPIKey = "NNSXS.LFKZMYHWIZAXYJQL4TK6PER3CKXX4XNSZULY4EA.YJ7B4GEBZVEDLG6UPTQWVSHL6MMGUVBM4O25XTTBGWGKWO4BUPXA"

# Note the path you have to specify. Double note that it has be prefixed with up.
theFields = "up.uplink_message.decoded_payload,up.uplink_message.frm_payload"

theNumberOfRecords = 10

theURL = "https://eu1.cloud.thethings.network/api/v3/as/applications/" + theApplication + "/packages/storage/uplink_message?order=-received_at&limit=" + str(theNumberOfRecords) + "&field_mask=" + theFields

# These are the headers required in the documentation. The Accept header needs
# further investigation as you can ask for other formats but it is ignored.
theHeaders = { 'Accept': 'text/event-stream', 'Authorization': 'Bearer ' + theAPIKey }

print("\n\nFetching from data storage  ...\n")

r = requests.get(theURL, headers=theHeaders)

print("URL: " + r.url)
print()

print("Status: " + str(r.status_code))
print()

print("Response: ")
print(r.text)
print()


# Due to some design choices by TTI, the text returned is not proper JSON.
# Event Stream (see headers above) is a connection type that sends well 
# formed blocks of JSON as a chunk or a message becomes available. We can't 
# subscribe to this stream due to CORS restrictions and if we ask for more 
# than one record, we are sent the chunks with a blank line between them and
 
theJSON = "[{\"data\": [" + r.text.replace("\n\n", ",")[:-1] + "]}]";

print("JSON: ")
parsedJSON = json.loads(theJSON)

with open('response.json', 'w') as f:
	json.dump(parsedJSON, f)

print(json.dumps(parsedJSON, indent=4))
print()

