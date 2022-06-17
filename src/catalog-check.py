import requests
from datetime import datetime
import re

# Check the Open Data portal for new datasets
OPENDATA_URL = 'https://www.opendatalapalma.es/api/v3/datasets'
OPENDATA_PARAMS = {
    'page[size]': 100,
    'filter[source]': 'Cabildo Insular de La Palma',
    'filter[downloadable]': 'true',
    'filter[hubType]': 'Feature Layer',
    'sort': '-created'
}

CLEANR = re.compile('<.*?>')

def cleanhtml(raw_html):
    cleantext = re.sub(CLEANR, '', raw_html).replace('\n', ' ').replace('\r', '')
    return cleantext

r = requests.get(OPENDATA_URL, OPENDATA_PARAMS)

if r.status_code != 200:
    print("Wrong request!")
else:
    results = r.json()['data']
    if len(results) == 0:
        print("No results in your search!")

print("\"id\",\"timestamp\",\"description\"")
for r in results:
    atts = r['attributes']
    desc = atts['description'] or atts['snippet']
    created = datetime.fromtimestamp(atts['created'] / 1000).isoformat()
    id = r['id']
    clean_desc = cleanhtml(desc) if desc else ''
    print(f"\"{id}\",\"{created}\",\"{clean_desc[:130]}\"")
