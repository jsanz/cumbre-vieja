import os
import sys
import requests

from datetime import datetime
from pytz import timezone

# import json
from geojson_rewind import rewind
from area import area

from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk

from dotenv import load_dotenv

load_dotenv()

# Define manually the datasets to download because
# the catalog is tricky to harvest
IDS = [
    ["f2c2135df71346bcb82dc018ce019422_0", "2021-11-05", "12:15"],
    ["5064c75858064595a601b17c2877ef10_0", "2021-11-04", "10:00"],
    ["f2f50f3bcb5a42eab1c56b26a606dec0_0", "2021-11-02", "13:00"],
    ["f2eb801094214c588c40106ba7a0716c_0", "2021-11-01", "09:30"],
    ["d2c0581b08644b4d8cd40f591439b8e7_0", "2021-10-31", "10:15"],
    ["2ca496dc59cc4ffe8470e1a034720cb2_0", "2021-10-28", "11:30"],
    ["128c373ea8634064b7efe2edfa6ee5e4_0", "2021-10-27", "11:30"],
    ["94e4b6609aff4a1cbbcb9a56ed5437b9_0", "2021-10-25", "10:30"],
    ["01f3ec70ebe74be88aa06efcee0d891d_0", "2021-10-23", "11:30"],
    ["817112ace83d489996ac5910306e26ba_0", "2021-10-22", "12:00"],
    ["2184470a8f7341bca0593c7191fccc00_0", "2021-10-21", "11:30"],
    ["507232606f8645909ed6863e4fe92247_0", "2021-10-20", "10:00"],
    ["45394ae50d894b369f31aa16787feffc_0", "2021-10-19", "15:30"],
    ["b2c4f01219234ba88d2dec90f55174a1_0", "2021-10-18", "11:30"],
    ["513d1de2dd0844b88240a65f490600dc_0", "2021-10-17", "12:00"],
    ["042e67a37e1f498082c91b03137a3e8e_0", "2021-10-16", "10:15"],
    ["1694172da82e448f82aa884a4de8892a_0", "2021-10-15", "11:00"],
    ["9f3a6d26474b4599996199dce3e09552_0", "2021-10-14", "13:30"],
    ["24bc16b5741649778d7710eea062ba37_0", "2021-10-14", "10:30"],
    ["9d68af3929734271bb6c7cfafd810a18_0", "2021-10-13", "15:30"],
    ["4603d9dda528406f84b003a88b829e4f_0", "2021-10-13", "12:00"],
    ["0c7a2e1b5e754058a1c22258bf121c7e_0", "2021-10-12", "13:00"],
    ["ce091a70278a45f4a9a7b857a7871586_0", "2021-10-11", "12:00"],
    ["2b8f01906eb946e2bb418ca4cbc7d40c_0", "2021-10-10", "19:00"],
    ["07a6b8b27bdb43d7afe8407fb0734d70_0", "2021-10-10", "12:00"],
    ["4d44773370c64f238e8cef7ff23c9619_0", "2021-10-09", "18:00"],
    ["8516a51114944fa6a28c5d31ccbe2787_0", "2021-10-09", "12:00"],
    ["37c2a63e7ccf49e7b984fd04ea3a0a5f_0", "2021-10-08", "16:00"],
    ["a9257c6f2dc34bb7aac82bdb0640e6cd_0", "2021-10-08", "12:00"],
    ["4f294b87db8142f28997923c2a405fdb_0", "2021-10-07", "12:00"],
    ["f82c1b14cd7844b78985e2e5652e1333_0", "2021-10-06", "19:00"],
    ["0485756faebc417db27ed6215984ec9b_0", "2021-10-05", "19:00"],
    ["e58f8c3cccad44af90e0c4e9f25caa1c_0", "2021-10-05", "12:00"],
    ["f8f18fbcfd27490abcaaa200405714e1_0", "2021-10-04", "17:00"],
    ["1ff24da6c0894eca998965b572ec5fe0_0", "2021-10-04", "11:00"],
    ["eeb31de1903d4025875eb8c934c1208a_0", "2021-10-02", "19:00"],
    ["c201734574ec4f6e85011e7d921d9bcc_0", "2021-10-02", "11:00"],
    ["5464cc4281e34d03a6ba5bdc6a877418_0", "2021-10-01", "18:00"],
    ["4dd2454d578a4b90b56e1413ce47578b_0", "2021-10-01", "11:00"],
    ["fddaa59746a3461eaf0540606361dc11_0", "2021-09-30", "18:00"],
    ["1624ee4de3b742fe8ae8ec6361916b4b_0", "2021-09-29", "12:00"],
    ["da404ef4096a4233a7784410bafece5d_0", "2021-09-28", "19:00"],
    ["bba7c1e51cb64d83803f04d1e48237e6_0", "2021-09-28", "12:00"],
    ["048d1640d4534b8faa86f039710ab5a5_0", "2021-09-27", "18:00"],
    ["0d94ac4ba5384b619470cb5638c05878_0", "2021-09-26", "13:00"],
    ["ad13943f3a2041039e671f5d7623c2ab_0", "2021-09-25", "18:00"],
    ["02a568616ee64cfa89ac5e8de941c132_0", "2021-09-24", "14:00"],
    ["3037f22a00344fbabb34f3f7c9d70ddc_0", "2021-09-20", "12:00"],
]

ALWAYS_UPLOAD = False

GEOJSON_URL = (
    "https://opendata.arcgis.com/api/v3/datasets/",
    "{id}/downloads/data?",
)
GEOJSON_PARAMS = {"format": "geojson", "spatialRefId": "4326"}
LOC_CANARY = timezone("Atlantic/Canary")

# Create the client
ES_CLOUD_ID = os.getenv("ES_CLOUD_ID")
ES_USER = os.getenv("ES_USER")
ES_PASSWORD = os.getenv("ES_PASSWORD")
if not (ES_CLOUD_ID and ES_PASSWORD and ES_USER):
    print("Environment variables missing")
    sys.exit(1)

es = Elasticsearch(cloud_id=ES_CLOUD_ID, http_auth=(ES_USER, ES_PASSWORD))

# Create the index if absent
if not es.indices.exists(index="lapalma"):
    es.indices.create(
        index="lapalma",
        settings={"number_of_shards": 1, "number_of_replicas": 1},
        mappings={
            "properties": {
                "geometry": {"type": "geo_shape"},
                "id": {"type": "text"},
                "timestamp": {"type": "date"},
                "area": {"type": "long"},
            }
        },
    )

# Loop the ids
features = []
for id_date in IDS:
    id = id_date[0]
    timestamp = LOC_CANARY.localize(
        datetime.strptime(f"{id_date[1]} {id_date[2]}", "%Y-%m-%d %H:%M")
    )

    try:
        es.get(index="lapalma", id=id)
        is_found = True
    except NotFoundError:
        is_found = False

    # Download the GeoJSON only if not found or always to upload
    if ALWAYS_UPLOAD or not is_found:
        print(f"Getting the resource [{id}]...")
        url = GEOJSON_URL.format(id=id)
        r = requests.get(url, GEOJSON_PARAMS)
        if r.status_code != 200:
            print(f"Resource [{id}]not found")
        else:
            json_dataset = r.json()
            if (
                "features" in json_dataset
                and "geometry" in json_dataset["features"][0]
            ):
                geometry = rewind(json_dataset["features"][0]["geometry"])
                geom_area = int(area(geometry))

                doc = {
                    "id": id,
                    "geometry": geometry,
                    "area": geom_area,
                    "timestamp": timestamp,
                }
                es.index(index="lapalma", id=id, document=doc)
                if ALWAYS_UPLOAD:
                    features.append(
                        {
                            "type": "Feature",
                            "geometry": doc["geometry"],
                            "id": id,
                            "properties": {
                                "id": id,
                                "timestamp": timestamp.isoformat(),
                                "area": geom_area,
                            },
                        }
                    )

"""# Earthquakes"""

EARTHQUAKE_URL = "https://www.ign.es/web/ign/portal/sis-catalogo-terremotos?"
EARTHQUAKE_URL_PARAMS = {
    "p_p_id": "IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet",
    "p_p_lifecycle": "2",
    "p_p_state": "normal",
    "p_p_mode": "view",
    "p_p_cacheability": "cacheLevelPage",
    "p_p_col_id": "column-1",
    "p_p_col_count": "1",
    "_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_jspPage": "%2Fjsp%2Fterremoto.jsp",
}

E_PARAMS = {
    "latMin": 28.436695,
    "latMax": 28.868729,
    "lonMin": -18.045731,
    "lonMax": -17.685928,
    "startDate": "01/08/2021",
    "endDate": datetime.strftime(datetime.now(), "%d/%m/%Y"),
}

EARTHQUAKE_FORM_DATA = "".join(
    [
        f'--data-raw $\'------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name="_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_formDate"\r\n\r\n1634805267580\r\n',
        f'------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name="_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_fases"\r\n\r\nno\r\n',
        f'------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name="_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_selIntensidad"\r\n\r\nN\r\n',
        f'------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name="_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_selMagnitud"\r\n\r\nN\r\n',
        f'------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name="_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_selProf"\r\n\r\nN\r\n',
        f"------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name=\"_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_latMin\"\r\n\r\n{E_PARAMS['latMin']}\r\n",
        f"------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name=\"_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_latMax\"\r\n\r\n{E_PARAMS['latMax']}\r\n",
        f"------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name=\"_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_longMin\"\r\n\r\n{E_PARAMS['lonMin']}\r\n",
        f"------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name=\"_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_longMax\"\r\n\r\n{E_PARAMS['lonMax']}\r\n",
        f"------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name=\"_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_startDate\"\r\n\r\n{E_PARAMS['startDate']}\r\n",
        f"------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name=\"_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_endDate\"\r\n\r\n{E_PARAMS['endDate']}\r\n",
        f'------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name="_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_intMin"\r\n\r\n\r\n',
        f'------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name="_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_intMax"\r\n\r\n\r\n',
        f'------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name="_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_magMin"\r\n\r\n\r\n',
        f'------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name="_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_magMax"\r\n\r\n\r\n',
        f'------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name="_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_cond"\r\n\r\n\r\n',
        f'------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name="_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_profMin"\r\n\r\n\r\n',
        f'------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name="_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_profMax"\r\n\r\n\r\n',
        f'------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name="_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_tipoDescarga"\r\n\r\ncsv\r\n',
        f"------WebKitFormBoundaryl7CMY2CM99CkEfej--\r\n",
    ]
)

r = requests.post(
    EARTHQUAKE_URL, params=EARTHQUAKE_URL_PARAMS, data=EARTHQUAKE_FORM_DATA
)
if r.status_code != 200:
    print("Wrong request!")
else:
    e_text = r.text
    rows = e_text.split("\r\n")[1:]
    print("Sample records")
    for row in rows[:5]:
        print(row)

print(f"\r\n{len(rows)} records")

# Create the index if absent
es.indices.delete(index="earthquakes")
es.indices.create(
    index="earthquakes",
    settings={"number_of_shards": 1, "number_of_replicas": 1},
    mappings={
        "properties": {
            "id": {"type": "text"},
            "timestamp": {"type": "date"},
            "geometry": {"type": "geo_shape"},
            "latitude": {"type": "float"},
            "longitude": {"type": "float"},
            "depth": {"type": "float"},
            "intensity": {"type": "keyword"},
            "magnitude": {"type": "float"},
            "mag_type": {"type": "keyword"},
            "location": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}},
            },
        }
    },
)

actions = []
for row in rows:
    parts = list(map(lambda x: x.strip(), row.split(";")))
    if len(parts) == 10:
        try:
            latitude = float(parts[3])
            longitude = float(parts[4])
            doc = {
                "id": parts[0],
                "timestamp": LOC_CANARY.localize(
                    datetime.strptime(
                        f"{parts[1]} {parts[2]}", "%d/%m/%Y %H:%M:%S"
                    )
                ),
                "geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude],
                },
                "latitude": latitude,
                "longitude": longitude,
                "depth": int(parts[5]) if parts[5] != "" else None,
                "intensity": parts[6],
                "magnitude": float(parts[7]),
                "mag_type": parts[8],
                "location": parts[9],
            }
            actions.append(
                {
                    "_index": "earthquakes",
                    "_op_type": "index",
                    "_id": parts[0],
                    "_source": doc,
                }
            )
        except Exception as e:
            print(e)
            print(parts)

print(f"Uploading to ES {len(actions)} records...")
bulk(es, actions)
