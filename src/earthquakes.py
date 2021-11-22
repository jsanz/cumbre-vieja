import logging
from datetime import datetime

import requests_cache
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError

from data import LOC_CANARY

INDEX_NAME = "earthquakes"

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("app")

session = requests_cache.CachedSession("http_cache", use_cache_dir=True)


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

    r = session.post(
        EARTHQUAKE_URL, params=EARTHQUAKE_URL_PARAMS, data=EARTHQUAKE_FORM_DATA
    )
    if r.status_code != 200:
        logger.error("Wrong request!")
    else:
        e_text = r.text
        rows = e_text.split("\r\n")[1:]

    return rows


def index_quakes(client, rows):
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
                actions.append(
                    {
                        "_index": "earthquakes",
                        "_op_type": "index",
                        "_id": parts[0],
                        "_source": doc,
                    }
                )
            except Exception as e:
                logger.error(e)
                logger.error(parts)

    logger.info(f"Uploading to ES {len(actions)} records...")
    bulk(client, actions)
