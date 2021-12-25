import sqlite3
import json
import ssl
import urllib.request, urllib.parse, urllib.error
from urllib.parse import urljoin
from urllib.parse import urlparse

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

url = "https://data.wa.gov/resource/769e-73q6.json"

json_data = None

try:
    # Open with a timeout of 30 seconds
    document = urllib.request.urlopen(url, None, 30, context=ctx)
    json_data = json.loads(document.read().decode())
    if document.getcode() != 200 :
        print("Error code=",document.getcode(), url)
except KeyboardInterrupt:
    print('')
    print('Program interrupted by user...')
except Exception as e:
    print("Unable to retrieve or parse page",url)
    print("Error",e)

conn = sqlite3.connect('raw_data.sqlite')
cur = conn.cursor()

cur.executescript('''
DROP TABLE IF EXISTS card_transfer;

CREATE TABLE card_transfer(
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    count_of_new_credential     INTEGER,
    year                        INTEGER,
    month                       INTEGER,
    issue_date                  TEXT,
    card_type_issued            TEXT,
    county                      TEXT,
    origin_state                TEXT,
    origin_country              TEXT,
    geocode                     BLOB
);
''')

conn.commit()
conn.close()