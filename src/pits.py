import logging
import warnings

from elasticsearch.exceptions import RequestError
from elasticsearch.client import IndicesClient
from elasticsearch.helpers.actions import bulk

from data import download_geojson


warnings.filterwarnings("ignore")
logging.getLogger("elasticsearch").setLevel(logging.ERROR)

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("app")

INDEX_NAME = "eruptive_pits"
GEOJSON_URL = (
    "https://opendata.arcgis.com/datasets/e3ea23b4d8cd40a4bda684bcc6d2d385_0.geojson"
)


def create_index(client):
    try:
        client.indices.create(
            index=INDEX_NAME,
            settings={"number_of_shards": 1, "number_of_replicas": 1},
            mappings={
                "properties": {
                    "OBJECTID": {"type": "long"},
                    "coordinates": {"type": "geo_point"},
                    "fecha": {"type": "date"}
                }
            }
        )

        return True
    except RequestError as e:
        logger.warning("Error creating index")
        logger.warning(e.info)


def get_actions(features):
    for feature in features:
        try:
            properties = feature["properties"]
            geometry = feature["geometry"]["coordinates"]

            id = properties["OBJECTID"]

            yield {
                "_index": INDEX_NAME,
                "_op_type": "index",
                "_id": str(id),
                "_source": {
                    "OBJECTID": id,
                    "coordinates": geometry,
                    "fecha": properties["fecha"]
                }
            }
        except Exception as e:
            logger.error(f"[{type(e)}] - {e}")


def upload_pits(client, overwrite=False):
    exists = IndicesClient(client).exists(INDEX_NAME)
    # Creates the pits index or exits
    if exists:
        if overwrite:
            logger.info("Deleting the pits index...")
            client.indices.delete(index=INDEX_NAME)
        else:
            logger.info("Skipping the pits")
            return

    logger.info("Creating the pits index...")
    create_index(client)

    logger.info("Getting the pits data...")
    features = download_geojson(GEOJSON_URL)
    logger.debug(f"{len(features)} pits downloaded")

    # Bulk upload the records
    logger.info(f"Uploading to ES {len(features)} records...")
    bulk(client, get_actions(features))
