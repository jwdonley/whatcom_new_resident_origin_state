import sqlite3
import json
import ssl
import urllib.request, urllib.parse, urllib.error
from urllib.parse import urljoin
from urllib.parse import urlparse
import hidden
import time

conn = sqlite3.connect('raw_data.sqlite')
cur = conn.cursor()

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Pick up where we left off
offset = None
cur.execute('SELECT COUNT(*) FROM card_transfer' )
try:
    row = cur.fetchone()
    if row is None :
        offset = 0
    else:
        offset = row[0]
except:
    offset = 0

if offset is None : offset = 0

print("Offset is", offset)

json_data = None

while True:
    url = "https://data.wa.gov/resource/769e-73q6.json?$$app_token=" + hidden.wa_gov_app_token + "&$limit=1000&$offset=" + str(offset)

    print("retrieving the next 1000 entries")
    try:
        # Open with a timeout of 30 seconds
        document = urllib.request.urlopen(url, None, 30, context=ctx)
        if document.getcode() != 200 :
            print("Error code=",document.getcode(), url)
            break
    except KeyboardInterrupt:
        print('')
        print('Program interrupted by user...')
        break
    except Exception as e:
        print("Unable to retrieve or parse page",url)
        print("Error",e)
        break

    json_data = json.loads(document.read().decode())
    if len(json_data) < 1:
        break
        
    for entry in json_data:
        print(entry)
        count_of_new_credential = entry["count_of_new_credential"]
        year = entry["year"]
        month = entry["month"]
        issue_date = entry["issue_date"]
        card_type_issued = entry["card_type_issued"]
        county = entry["county_of_residence"]
        origin_state = entry["state_of_origin"]
        origin_country = entry["country_of_origin"]
        geocode_type = entry["geocoded_column"]["type"]
        geocode_x = entry["geocoded_column"]["coordinates"][0]
        geocode_y = entry["geocoded_column"]["coordinates"][1]

        cur.execute('''INSERT INTO card_transfer (count_of_new_credential, year, month, issue_date, card_type_issued, county, origin_state, origin_country, geocode_type, geocode_x, geocode_y)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (count_of_new_credential, year, month, issue_date, card_type_issued, county, origin_state, origin_country, geocode_type, geocode_x, geocode_y))

    conn.commit()
    offset += 1000;
    time.sleep(5)

conn.close()