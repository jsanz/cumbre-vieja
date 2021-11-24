from copy import deepcopy
import json
import logging
import warnings
from datetime import datetime

import requests_cache
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError

from data import LOC_CANARY

INDEX_NAME = "earthquakes"

warnings.filterwarnings("ignore")
logging.getLogger("elasticsearch").setLevel(logging.ERROR)

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("app")

session = requests_cache.CachedSession("http_cache", use_cache_dir=True)


def get_quake(row):
    parts = list(map(lambda x: x.strip(), row.split(";")))
    if len(parts) == 10:
        try:
            latitude = float(parts[3])
            longitude = float(parts[4])
            return {
                "id": parts[0],
                "timestamp": LOC_CANARY.localize(
                    datetime.strptime(f"{parts[1]} {parts[2]}", "%d/%m/%Y %H:%M:%S")
                ),
                "geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude],
                },
                "latitude": latitude,
                "longitude": longitude,
                "depth": float(parts[5]) if parts[5] != "" else None,
                "intensity": parts[6],
                "magnitude": float(parts[7]),
                "mag_type": parts[8],
                "location": parts[9],
            }
        except Exception as e:
            logger.error(e)
            logger.error(parts)
            return None
    else:
        return None


def download_earthquakes():
    """
    Downloads the Earthquakes data from the Spanish National Mapping Agency
    """
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

    FORM_STRING = '------WebKitFormBoundaryl7CMY2CM99CkEfej\r\nContent-Disposition: form-data; name="_IGNSISCatalogoTerremotos_WAR_IGNSISCatalogoTerremotosportlet_'

    form_variables = {
        "formDate": "1634805267580",
        "fases": "no",
        "selIntensidad": "N",
        "selMagnitud": "N",
        "selProf": "N",
        "latMin": "28.436695",
        "latMax": "28.868729",
        "longMin": "-18.045731",
        "longMax": "-17.685928",
        "startDate": "01/08/2021",
        "endDate": datetime.strftime(datetime.now(), "%d/%m/%Y"),
        "intMin": "",
        "intMax": "",
        "magMin": "",
        "magMax": "",
        "cond": "",
        "profMin": "",
        "profMax": "",
        "tipoDescarga": "csv",
    }

    # Expand the variables with the FORM_STRING template
    form_data = (
        "--data-raw $"
        + "".join(
            map(
                lambda p: FORM_STRING + p + '"\r\n\r\n' + form_variables[p] + "\r\n",
                form_variables.keys(),
            )
        )
        + "------WebKitFormBoundaryl7CMY2CM99CkEfej--\r\n"
    )

    r = session.post(EARTHQUAKE_URL, params=EARTHQUAKE_URL_PARAMS, data=form_data)

    if r.status_code != 200:
        logger.error("Wrong request!")
    else:
        e_text = r.text
        rows = e_text.split("\r\n")[1:]

    logger.debug(f"{len(rows)} rows returned")

    filtered_quakes = filter(lambda q: q is not None, map(get_quake, rows))

    return list(filtered_quakes)


def index_quakes(client, quakes):

    # Only repopulate if the number of quakes is higher than the index doc count
    count_obj = client.count(index=INDEX_NAME)
    if "count" in count_obj and count_obj["count"] == len(quakes):
        logger.info('Index has the same number of documents than downloaded data, skipping')
        return

    """
    Recreates the index for the Earthquakes and uploads the data
    """
    # Create the index if absent
    try:
        client.indices.delete(index=INDEX_NAME)
    except NotFoundError:
        logger.debug("Index not found, nothing to delete")
    client.indices.create(
        index=INDEX_NAME,
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

    actions = list(
        map(
            lambda quake: {
                "_index": "earthquakes",
                "_op_type": "index",
                "_id": quake["id"],
                "_source": quake,
            },
            quakes,
        )
    )

    logger.info(f"Uploading to ES {len(actions)} records...")
    bulk(client, actions)


def get_geojson_feature(feature):
    properties = deepcopy(feature)
    geometry = properties.pop("geometry")

    for key in properties.keys():
        if type(properties[key]) is datetime:
            properties[key] = (properties[key].isoformat(),)

    return {"type": "Feature", "geometry": geometry, "properties": properties}


def export(features):
    """
    Creates a GeoJSON for the earthquakes
    """

    FILE_PATH = "/tmp/earthquakes.geo.json"

    with open(FILE_PATH, "w") as writer:
        f_features = map(get_geojson_feature, features)
        logger.debug(f"Exporting earthquakes GeoJSON into {FILE_PATH}...")
        json.dump({"type": "FeatureCollection", "features": list(f_features)}, writer)
