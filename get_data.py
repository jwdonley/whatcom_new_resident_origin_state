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

print(json_data[0])